#include <iostream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <queue>
#include <cstring>
#include <fstream>
#include <climits>
#include <atomic>
#include <functional>
#include <utility>
#include <numeric>
#include <cmath>

#include "multiqueue.h"

#ifdef __linux__
#include <sched.h>
#endif

using Vertex = std::size_t;
using DistType = int;
using DistVector = std::vector<DistType>;
using AtomicDistVector = std::vector<std::atomic<DistType>>;

class Edge {
private:
    Vertex to;
    DistType weight;
public:
    Edge(Vertex to, DistType weight) : to(to), weight(weight) {}
    Vertex get_to() const {
        return to;
    }
    DistType get_weight() const {
        return weight;
    }
};

using AdjList = std::vector<std::vector<Edge>>;


template <class T>
class AbstractQueue {
public:
    virtual void push(T elem) = 0;
    virtual std::size_t get_num_pushes() {
        return 0;
    }
    virtual std::vector<std::size_t> get_max_queue_sizes() {
        return std::vector<std::size_t>();
    }
    virtual T pop() = 0;
    virtual ~AbstractQueue() = default;
};

template <class T>
class RegularPriorityQueue : public AbstractQueue<T> {
private:
    std::priority_queue<T> queue;
    const T empty_element;
public:
    explicit RegularPriorityQueue(T empty_element) : empty_element(empty_element) {}
    void push(T elem) override {
        queue.push(elem);
    }
    T pop() override {
        if (queue.empty()) {
            return empty_element;
        }
        T elem = queue.top();
        queue.pop();
        return elem;
    }
};

template <class T>
class BlockingQueue : public AbstractQueue<T> {
private:
    std::priority_queue<T> queue;
    std::mutex mutex;
    const T empty_element;
public:
    explicit BlockingQueue(T empty_element) : empty_element(empty_element) {}
    void push(T elem) override {
        std::lock_guard<std::mutex> lock(mutex);
        queue.push(elem);
    }
    T pop() override {
        std::lock_guard<std::mutex> lock(mutex);
        if (queue.empty()) {
            return empty_element;
        }
        T elem = queue.top();
        queue.pop();
        return elem;
    }
};

template <class T>
class MultiQueue : public AbstractQueue<T> {
private:
    Multiqueue<T> queue;
    T empty_element;
public:
    MultiQueue(const int num_threads, const int size_multiple, T empty_element, std::size_t one_queue_reserve_size,
            bool use_try_lock, bool collect_statistics) :
            queue(num_threads, size_multiple, empty_element, one_queue_reserve_size, use_try_lock, collect_statistics),
            empty_element(empty_element) {}
    void push(T elem) override {
        queue.push(elem);
    }
    std::size_t get_num_pushes() override {
        return queue.get_num_pushes();
    }
    std::vector<std::size_t> get_max_queue_sizes() override {
        return queue.get_max_queue_sizes();
    }
    T pop() override {
        return queue.pop();
    }
};

class QueueElement {
private:
    Vertex vertex;
    DistType dist;
public:
    QueueElement(Vertex vertex, DistType dist) : vertex(vertex), dist(dist) {}
    Vertex get_vertex() const {
        return vertex;
    }
    DistType get_dist() const {
        return dist;
    }
    bool operator==(const QueueElement & o) const {
        return o.get_vertex() == vertex && o.get_dist() == dist;
    }
    bool operator!=(const QueueElement & o) const {
        return !operator==(o);
    }
    bool operator<(const QueueElement & o) const {
        return dist > o.get_dist();
    }
};

using QueueFactory = std::function<std::unique_ptr<AbstractQueue<QueueElement>>()>;

static const DistType EMPTY_ELEMENT_DIST = -1;
static const QueueElement EMPTY_ELEMENT = {0, EMPTY_ELEMENT_DIST};

class SsspDijkstraDistsAndStatistics {
private:
    DistVector dists;
    DistVector vertex_pulls_counts;
    std::size_t num_pushes;
    std::vector<std::size_t> max_queue_sizes;
public:
    SsspDijkstraDistsAndStatistics(
            DistVector dists, DistVector vertex_pulls_counts, size_t num_pushes,
            std::vector<std::size_t> max_queue_sizes) :
            dists(std::move(dists)), vertex_pulls_counts(std::move(vertex_pulls_counts)), num_pushes(num_pushes),
            max_queue_sizes(std::move(max_queue_sizes)) {}

    const DistVector &get_dists() const {
        return dists;
    }
    const DistVector &get_vertex_pulls_counts() const {
        return vertex_pulls_counts;
    }
    std::size_t get_num_pushes() const {
        return num_pushes;
    }
    const std::vector<std::size_t> &get_max_queue_sizes() const {
        return max_queue_sizes;
    }
};

void thread_routine(const AdjList & graph, AbstractQueue<QueueElement> & queue, AtomicDistVector & dists,
        AtomicDistVector & vertex_pull_counts, bool collect_statistics) {
    while (true) {
        QueueElement elem = queue.pop();
        // TODO: fix that most treads might exit if one thread is stuck at cut-vertex
        if (elem.get_dist() == EMPTY_ELEMENT_DIST) {
//            std::cerr << "bye" << std::endl;
            break;
        }
        Vertex v = elem.get_vertex();
        DistType v_dist = elem.get_dist();
        DistType v_global_dist = dists[v];
        if (v_dist > v_global_dist) {
            continue;
        }
        if (collect_statistics) {
            vertex_pull_counts[v]++;
        }
        for (Edge e : graph[v]) {
            Vertex v2 = e.get_to();
            if (v == v2) continue;
            DistType new_v2_dist = v_dist + e.get_weight();
            while (true) {
                DistType old_v2_dist = dists[v2];
                if (old_v2_dist <= new_v2_dist) {
                    break;
                }
                if (dists[v2].compare_exchange_strong(old_v2_dist, new_v2_dist)) {
                    queue.push({v2, new_v2_dist});
                    break;
                }
            }
        }
    }
}

template<class T>
std::vector<std::atomic<T>> initialize(std::size_t n, const T & x) {
    std::vector<std::atomic<T>> atomic_vector(n);
    for (auto & atomic_element : atomic_vector) {
        atomic_element = x;
    }
    return atomic_vector;
}

template<class T>
std::vector<T> unwrap_from_atomic(const std::vector<std::atomic<T>> & atomic_vector) {
    std::vector<T> regular_vector;
    regular_vector.reserve(atomic_vector.size());
    for (const auto & atomic_element : atomic_vector) {
        regular_vector.push_back(atomic_element);
    }
    return regular_vector;
}

SsspDijkstraDistsAndStatistics calc_sssp_dijkstra(const AdjList & graph, std::size_t start_vertex,
        std::size_t num_threads, const QueueFactory & queue_factory, bool collect_statistics) {
    std::size_t num_vertexes = graph.size();
    auto queue_ptr = queue_factory();
    AbstractQueue<QueueElement> & queue = *queue_ptr;
    queue.push({start_vertex, 0});
    AtomicDistVector atomic_dists = initialize(num_vertexes, INT_MAX);
    atomic_dists[0] = 0;
    AtomicDistVector atomic_vertex_pull_counts = initialize(num_vertexes, 0);
    std::vector<std::thread> threads;
    for (std::size_t i = 0; i < num_threads; i++) {
        threads.emplace_back(thread_routine, std::cref(graph), std::ref(queue), std::ref(atomic_dists),
                std::ref(atomic_vertex_pull_counts), collect_statistics);
        #ifdef __linux__
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(i, &cpuset);
            int rc = pthread_setaffinity_np(threads.back().native_handle(), sizeof(cpu_set_t), &cpuset);
            (void)rc;
        #endif
    }
    for (std::thread & thread : threads) {
        thread.join();
    }
    DistVector dists = unwrap_from_atomic(atomic_dists);
    DistVector vertex_pulls_counts = unwrap_from_atomic(atomic_vertex_pull_counts);
    std::size_t num_pushes = queue.get_num_pushes();
    auto max_queue_sizes = queue.get_max_queue_sizes();
    return {dists, vertex_pulls_counts, num_pushes, max_queue_sizes};
}

SsspDijkstraDistsAndStatistics calc_sssp_dijkstra_sequential(const AdjList & graph, std::size_t start_vertex) {
    std::size_t num_vertexes = graph.size();
    DistVector dists(num_vertexes, INT_MAX);
    std::vector<bool> removed_from_queue(num_vertexes, false);
    std::priority_queue<QueueElement> q;
    dists[start_vertex] = 0;
    q.push({start_vertex, 0});
    for (std::size_t i = 0; i < num_vertexes; i++) {
        while (!q.empty() && removed_from_queue[q.top().get_vertex()]) {
            q.pop();
        }
        if (q.empty()) {
            break;
        }
        Vertex from = q.top().get_vertex();
        DistType dist = q.top().get_dist();
        q.pop();
        removed_from_queue[from] = true;
        for (const Edge & edge: graph[from]) {
            Vertex to = edge.get_to();
            DistType new_dist = dist + edge.get_weight();
            if (dists[to] > new_dist) {
                dists[to] = new_dist;
                q.push({to, new_dist});
            }
        }
    }
    return {dists, DistVector(num_vertexes, 1), num_vertexes,
            std::vector<std::size_t>(num_vertexes)};
}

AdjList gen_layer_graph(std::size_t n = 100000, bool bidirected = false, int weight = 1) {
    int sqrt_n = std::sqrt(n);
    int num_vertexes = sqrt_n * sqrt_n + 2;
    int source = 0;
    int sink = num_vertexes - 1;
    AdjList graph(num_vertexes);
    int first_layer = 1;
    for (int to = first_layer; to < first_layer + sqrt_n; to++) {
        int from = source;
        graph[from].emplace_back(to, weight);
        if (bidirected) {
            graph[to].emplace_back(0, weight);
        }
    }
    for (int layer = 0; layer < sqrt_n - 1; layer++) {
        int cur_layer = 1 + layer * sqrt_n;
        int next_layer = 1 + (layer + 1) * sqrt_n;
        for (int from = cur_layer; from < cur_layer + sqrt_n; from++) {
            for (int to = next_layer; to < next_layer + sqrt_n; to++) {
                graph[from].emplace_back(to, weight);
                if (bidirected) {
                    graph[to].emplace_back(from, weight);
                }
            }
        }
    }
    int last_layer = 1 + (sqrt_n - 1) * sqrt_n;
    for (int from = last_layer; from < last_layer + sqrt_n; from++) {
        int to = sink;
        graph[from].emplace_back(to, weight);
        if (bidirected) {
            graph[to].emplace_back(from, weight);
        }
    }
    return graph;
}

AdjList read_adj_matrix_into_adj_list(std::istream & istream) {
    std::size_t num_verticies;
    istream >> num_verticies;
    AdjList adj_matrix(num_verticies);
    for (Vertex from = 0; from < num_verticies; from++) {
        for (Vertex to = 0; to < num_verticies; to++) {
            DistType weight;
            istream >> weight;
            if (weight == 0) continue;
            adj_matrix[from].emplace_back(to, weight);
        }
    }
    return adj_matrix;
}

AdjList read_edges_into_adj_list(std::istream & istream, int vertex_numeration_offset = 0) {
    std::size_t num_verticies, num_edges;
    istream >> num_verticies >> num_edges;
    std::cerr << "n = " << num_verticies << ", m = " <<  num_edges << std::endl;
    AdjList adj_list(num_verticies);
    for (std::size_t i = 0; i < num_edges; i++) {
        Vertex from, to;
        DistType weight;
        istream >> from >> to >> weight;
        if (weight <= 0) continue;
        adj_list[from + vertex_numeration_offset].emplace_back(to + vertex_numeration_offset, weight);
    }
    return adj_list;
}

void write_answer(std::ostream & ostream, const DistVector & dists) {
    for (DistType dist : dists) {
        ostream << dist << '\n';
    }
    ostream << '\n';
}

bool are_mismatched(const DistVector & correct_answer, const DistVector & to_check) {
    auto mismatch = std::mismatch(correct_answer.begin(), correct_answer.end(), to_check.begin());
    if (mismatch.first != correct_answer.end()) {
        std::cerr << "Mismatch: " << *mismatch.first << " != " << *mismatch.second << std::endl;
        return true;
    }
    return false;
}

template<class R>
std::pair<R, std::chrono::milliseconds> measure_time(std::function<R()> func_to_test, std::size_t num_iterations = 1) {
    auto start = std::chrono::high_resolution_clock::now();
    for (std::size_t i = 0; i < num_iterations - 1; i++) {
        func_to_test();
    }
    R r = func_to_test();
    auto end = std::chrono::high_resolution_clock::now();
    return {r, std::chrono::duration_cast<std::chrono::milliseconds>(end - start) / num_iterations};
}

void read_run_measure(const std::string & filename,
        const std::vector<std::pair<std::function<SsspDijkstraDistsAndStatistics(const AdjList &, bool)>, std::string>>
        & dijkstra_implementations, std::size_t num_iterations) {
    std::ifstream input(filename + ".in");
    AdjList graph = read_edges_into_adj_list(input, -1);
    std::cerr << "Num iterations: " << num_iterations << std::endl;
    for (const auto & dijkstra_implementation : dijkstra_implementations) {
        const auto &f = dijkstra_implementation.first;
        const std::string &impl_name = dijkstra_implementation.second;

        auto p = measure_time<SsspDijkstraDistsAndStatistics>(
                [& f, & graph]() { return f(graph, false); }, num_iterations);
        auto avg_time_ms = p.second;

        std::cerr << impl_name << " avg elapsed time: " << avg_time_ms.count() << " ms" << std::endl;
    }
}

void read_run_check_write(const std::string & filename, std::size_t gen_graph_size,
        const std::vector<std::pair<std::function<SsspDijkstraDistsAndStatistics(const AdjList &, bool)>, std::string>>
        & dijkstra_implementations, bool run_only, bool collect_statistics) {
    AdjList graph;
    if (gen_graph_size > 0) {
        graph = gen_layer_graph(gen_graph_size);
    } else {
        std::ifstream input(filename + ".in");
        std::cerr << "Reading " << filename << ", ";
        auto p = measure_time<AdjList>([& input](){ return read_edges_into_adj_list(input, -1); });
        graph = p.first;
        std::chrono::milliseconds time_ms = p.second;
        std::cerr << "Reading elapsed time: " << time_ms.count() << " ms" << std::endl;
        std::cerr << std::endl;
    }

    DistVector correct_answer;
    std::size_t num_vertexes = graph.size();
    for (std::size_t i = 0; i < dijkstra_implementations.size(); i++) {
        const auto & f = dijkstra_implementations[i].first;
        const std::string & impl_name = dijkstra_implementations[i].second;

        auto p = measure_time<SsspDijkstraDistsAndStatistics>([& f, & graph, collect_statistics](){ return f(graph, collect_statistics); });
        auto dists_and_statistics = p.first;
        auto time_ms = p.second;

        std::cerr << impl_name << " elapsed time: " << time_ms.count() << " ms" << std::endl;
        if (run_only) continue;

        const DistVector &dists = dists_and_statistics.get_dists();
        if (collect_statistics) {
            const DistVector &vertex_pulls_counts = dists_and_statistics.get_vertex_pulls_counts();
            std::size_t num_pushes = dists_and_statistics.get_num_pushes();
            const auto &max_queue_sizes = dists_and_statistics.get_max_queue_sizes();
            std::size_t vertex_pulls_sum = std::accumulate(vertex_pulls_counts.begin(), vertex_pulls_counts.end(), 0);
            double overhead = 1.0 * vertex_pulls_sum / num_vertexes;
            std::size_t useless_pushes = num_pushes - vertex_pulls_sum;

            std::vector<int> vertex_to_num_edges(num_vertexes);
            for (std::size_t v = 0; v < num_vertexes; v++) {
                vertex_to_num_edges[v] = graph[v].size();
            }
            unsigned sequential_weighted_vertex_pulls = std::accumulate(vertex_to_num_edges.begin(),
                                                                        vertex_to_num_edges.end(), 0);
            unsigned weighted_vertex_pulls = std::inner_product(vertex_pulls_counts.begin(), vertex_pulls_counts.end(),
                                                                vertex_to_num_edges.begin(), 0);
            double weighted_overhead = 1.0 * weighted_vertex_pulls / sequential_weighted_vertex_pulls;
            std::size_t max_queue_size = *std::max_element(max_queue_sizes.begin(), max_queue_sizes.end());

            std::cerr << "Pulls: " << vertex_pulls_sum << " (" << overhead << "x)" << std::endl;
            std::cerr << "Pushes: " << num_pushes << std::endl;
            std::cerr << "Useless pushes:  " << useless_pushes << std::endl;
            std::cerr << "Edges accessed: " << weighted_vertex_pulls << " (" << weighted_overhead << "x)" << std::endl;
            std::cerr << "Max queue size: " << max_queue_size << std::endl;
            std::cerr << std::endl;
        }

        bool mismatched = false;
        if (i == 0) {
            correct_answer = dists;
        } else {
            mismatched = are_mismatched(correct_answer, dists);
        }

        if (mismatched) {
            std::ofstream output(filename + ".out" + std::to_string(i));
            auto start = std::chrono::high_resolution_clock::now();
            write_answer(output, dists);
            auto finish = std::chrono::high_resolution_clock::now();
            auto elapsed = finish - start;
            std::cerr << "Writing elapsed time: " << elapsed.count() << " s" << std::endl;
        }
    }
}

std::vector<std::pair<int, int>> read_params(const std::string & params_filename) {
    std::vector<std::pair<int, int>> params;
    std::ifstream params_input(params_filename);
    int num_threads;
    int size_multiple;
    while (params_input >> num_threads >> size_multiple) {
        params.emplace_back(num_threads, size_multiple);
    }
    return params;
}

int main(int argc, char *argv[]) {
    std::ios_base::sync_with_stdio(false);

    if (argc != 11) {
        std::cerr << "Usage: ./dijkstra input_filename_no_ext params_filename one_queue_reserve_size use_try_lock[0,1] "
                     "run_blocking_queue[0,1] run_regular_queue[0,1] gen_graph_size run_only[0,1] "
                     "collect_statistics[0,1] num_iterations"
        << std::endl;
        exit(1);
    }
    const std::string input_filename_no_ext(argv[1]);
    const std::string params_filename(argv[2]);
    const std::size_t one_queue_reserve_size = std::stoul(argv[3]);
    const bool use_try_lock = std::stoi(argv[4]);
    const bool run_blocking_queue = std::stoi(argv[5]);
    const bool run_regular_queue = std::stoi(argv[6]);
    const std::size_t gen_graph_size = std::stoi(argv[7]);
    const bool run_only = std::stoi(argv[8]);
    const bool collect_statistics = std::stoi(argv[9]);
    const std::size_t num_iterations = std::stoi(argv[10]);

    Vertex start_vertex = 0;
    std::vector<std::pair<int, int>> params = read_params(params_filename);

    std::vector<std::pair<std::function<SsspDijkstraDistsAndStatistics(const AdjList &, bool)>, std::string>>
            dijkstra_implementations;
    std::vector<QueueFactory> queue_factories;
    auto sequential_dijkstra = [start_vertex](const AdjList &graph, bool collect_statistics) {
            return calc_sssp_dijkstra_sequential(graph, start_vertex); (void)collect_statistics; };
    dijkstra_implementations.emplace_back(sequential_dijkstra, "Sequential");
    if (run_blocking_queue) {
        queue_factories.emplace_back([](){ return std::make_unique<BlockingQueue<QueueElement>>(EMPTY_ELEMENT); });
        const auto & blocking_queue_factory = queue_factories.back();
        for (const auto & param: params) {
            int num_threads = param.first;
            auto f = [start_vertex, num_threads, & blocking_queue_factory](const AdjList &graph, bool collect_statistics) {
                return calc_sssp_dijkstra(graph, start_vertex, num_threads, blocking_queue_factory, collect_statistics);
            };
            std::string impl_name = "BlockingQueue " + std::to_string(num_threads);
            dijkstra_implementations.emplace_back(f, impl_name);
        }
    }
    if (run_regular_queue) {
        queue_factories.emplace_back([](){ return std::make_unique<RegularPriorityQueue<QueueElement>>(EMPTY_ELEMENT); });
        const auto & regular_queue_factory = queue_factories.back();
        auto f = [start_vertex, & regular_queue_factory](const AdjList &graph, bool collect_statistics) {
            return calc_sssp_dijkstra(graph, start_vertex, 1, regular_queue_factory, collect_statistics);
        };
        dijkstra_implementations.emplace_back(f, "RegularQueue");
    }
    for (const auto & param: params) {
        int num_threads = param.first;
        int size_multiple = param.second;
        queue_factories.emplace_back([num_threads, size_multiple, one_queue_reserve_size, use_try_lock, collect_statistics]()
                { return std::make_unique<MultiQueue<QueueElement>>
                  (num_threads, size_multiple, EMPTY_ELEMENT, one_queue_reserve_size, use_try_lock, collect_statistics); });
        const auto & multi_queue_factory = queue_factories.back();
        std::string impl_name = "Multiqueue " + std::to_string(num_threads) + " " + std::to_string(size_multiple);
        dijkstra_implementations.emplace_back([start_vertex, num_threads, multi_queue_factory] (const AdjList & graph, bool collect_statistics)
            { return calc_sssp_dijkstra(graph, start_vertex, num_threads, multi_queue_factory, collect_statistics); }, impl_name);
    }

    if (num_iterations > 1) {
        read_run_measure(input_filename_no_ext, dijkstra_implementations, num_iterations);
    } else {
        read_run_check_write(input_filename_no_ext, gen_graph_size, dijkstra_implementations, run_only,
                             collect_statistics);
    }

}
