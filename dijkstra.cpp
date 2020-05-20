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

#include "multiqueue.h"

#ifdef __linux__
#define _GNU_SOURCE
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
    virtual T pop() = 0;
    virtual T get_empty_element() = 0;
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
    T get_empty_element() override {
        return empty_element;
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
    T get_empty_element() override {
        return empty_element;
    }
};

template <class T>
class MultiQueue : public AbstractQueue<T> {
private:
    Multiqueue<T> queue;
    T empty_element;
public:
    MultiQueue(const int num_threads, const int size_multiple, T empty_element) :
            queue(num_threads, size_multiple, empty_element), empty_element(empty_element) {}
    void push(T elem) override {
        queue.push(elem);
    }
    T pop() override {
        return queue.pop();
    }
    T get_empty_element() override {
        return empty_element;
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

void thread_routine(const AdjList & graph, AbstractQueue<QueueElement> & queue, AtomicDistVector & dists,
        AtomicDistVector & vertex_pull_counts) {
    while (true) {
        QueueElement elem = queue.pop();
        // TODO: fix that most treads might exit if one thread is stuck at cut-vertex
        if (elem == queue.get_empty_element()) {
//            std::cerr << "bye" << std::endl;
            break;
        }
        Vertex v = elem.get_vertex();
        DistType v_dist = elem.get_dist();
        DistType v_global_dist = dists[v];
        if (v_dist > v_global_dist) {
            continue;
        }
        vertex_pull_counts[v]++;
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

std::pair<DistVector, DistVector> calc_sssp_dijkstra(const AdjList & graph, std::size_t start_vertex,
        std::size_t num_threads, AbstractQueue<QueueElement> & queue) {
    queue.push({start_vertex, 0});
    AtomicDistVector atomic_dists = initialize(graph.size(), INT_MAX);
    atomic_dists[0] = 0;
    AtomicDistVector atomic_vertex_pull_counts = initialize(graph.size(), 0);
    std::vector<std::thread> threads;
    for (std::size_t i = 0; i < num_threads; i++) {
        threads.emplace_back(thread_routine, std::cref(graph), std::ref(queue), std::ref(atomic_dists),
                std::ref(atomic_vertex_pull_counts));
        #ifdef __linux__
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(i, &cpuset);
            int rc = pthread_setaffinity_np(threads.back().native_handle(), sizeof(cpu_set_t), &cpuset);
        #endif
    }
    for (std::thread & thread : threads) {
        thread.join();
    }
    DistVector dists = unwrap_from_atomic(atomic_dists);
    DistVector vertex_pulls_counts = unwrap_from_atomic(atomic_vertex_pull_counts);
    delete &queue;
    return std::make_pair(dists, vertex_pulls_counts);
}

std::pair<DistVector, DistVector> calc_sssp_dijkstra_sequential(const AdjList & graph, std::size_t start_vertex) {
    DistVector dists(graph.size(), INT_MAX);
    std::vector<bool> removed_from_queue(graph.size(), false);
    std::priority_queue<QueueElement> q;
    dists[start_vertex] = 0;
    q.push({start_vertex, 0});
    for (std::size_t i = 0; i < graph.size(); i++) {
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
    return std::make_pair(dists, DistVector(graph.size(), 1));
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

void read_run_check_write(const std::string & filename,
                          const std::vector<std::pair<std::function<std::pair<DistVector, DistVector>(const AdjList &)>,
                                  std::string>> & dijkstra_implementations) {
    auto start = std::chrono::high_resolution_clock::now();
    std::ifstream input(filename + ".in");
    std::cerr << "Reading " << filename << ", ";
    AdjList graph = read_edges_into_adj_list(input, -1);
    auto finish = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = finish - start;
    std::cerr << "Reading elapsed time: " << elapsed.count() << " s" << std::endl;
    std::cerr << std::endl;

    DistVector correct_answer;
    for (std::size_t i = 0; i < dijkstra_implementations.size(); i++) {
        const auto & f = dijkstra_implementations[i].first;
        const std::string & impl_name = dijkstra_implementations[i].second;
        start = std::chrono::high_resolution_clock::now();
        auto distsAndCounts = f(graph);
        finish = std::chrono::high_resolution_clock::now();
        elapsed = finish - start;

        DistVector dists = distsAndCounts.first;
        DistVector vertex_pulls_counts = distsAndCounts.second;
        unsigned vertex_pulls_sum = std::accumulate(vertex_pulls_counts.begin(), vertex_pulls_counts.end(), 0);
        double overhead = 1.0 * vertex_pulls_sum / graph.size();

        std::vector<int> vertex_to_num_edges(graph.size());
        for (std::size_t j = 0; j < graph.size(); j++) {
            vertex_to_num_edges[j] = graph[j].size();
        }
        unsigned sequential_weighted_vertex_pulls = std::accumulate(vertex_to_num_edges.begin(), vertex_to_num_edges.end(), 0);
        unsigned weighted_vertex_pulls = std::inner_product(vertex_pulls_counts.begin(), vertex_pulls_counts.end(),
                                                            vertex_to_num_edges.begin(), 0);
        double weighted_overhead = 1.0 * weighted_vertex_pulls / sequential_weighted_vertex_pulls;

        std::cerr << impl_name << " elapsed time: " << elapsed.count() << " s" << std::endl;
        std::cerr << "Vertex pulls: " << vertex_pulls_sum << " (" << overhead << "x)" << std::endl;
        std::cerr << "Weighted vertex pulls: " << weighted_vertex_pulls << " (" << weighted_overhead << "x)" << std::endl;
        std::cerr << std::endl;

        bool mismatched = false;
        if (i == 0) {
            correct_answer = dists;
        } else {
            mismatched = are_mismatched(correct_answer, dists);
        }

        if (mismatched) {
            std::ofstream output(filename + ".out" + std::to_string(i));
            start = std::chrono::high_resolution_clock::now();
            write_answer(output, dists);
            finish = std::chrono::high_resolution_clock::now();
            elapsed = finish - start;
            std::cerr << "Writing elapsed time: " << elapsed.count() << " s" << std::endl;
        }
    }
}

int main(int argc, char *argv[]) {
    std::ios_base::sync_with_stdio(false);

    if (argc != 5) {
        std::cerr << "Usage: ./dijkstra input_filename_no_ext params_filename run_blocking_queue[0,1] "
                     "run_regular_queue[0,1]"
        << std::endl;
        exit(1);
    }
    std::string input_filename_no_ext(argv[1]);
    std::string params_filename(argv[2]);
    bool run_blocking_queue = std::stoi(argv[3]);
    bool run_regular_queue = std::stoi(argv[4]);

    Vertex start_vertex = 0;
    QueueElement empty_element = {start_vertex, -1};
    std::vector<std::pair<int, int>> params;
    std::ifstream params_input(params_filename);
    while (params_input.good()) {
        int num_threads;
        int size_multiple;
        params_input >> num_threads >> size_multiple;
        params.emplace_back(num_threads, size_multiple);
    }

    std::vector<std::pair<std::function<std::pair<DistVector, DistVector>(const AdjList &)>, std::string>>
            dijkstra_implementations;
    {
        auto f = [start_vertex](const AdjList &graph) { return calc_sssp_dijkstra_sequential(graph, start_vertex); };
        dijkstra_implementations.emplace_back(f, "Sequential");
    }
    if (run_blocking_queue) {
        for (const auto & param: params) {
            int num_threads = param.first;
            AbstractQueue<QueueElement> *blocking_queue = new BlockingQueue<QueueElement>(empty_element);
            auto f = [start_vertex, num_threads, blocking_queue](const AdjList &graph) {
                return calc_sssp_dijkstra(graph, start_vertex, num_threads, *blocking_queue);
            };
            std::string impl_name = "BlockingQueue " + std::to_string(num_threads);
            dijkstra_implementations.emplace_back(f, impl_name);
        }
    }
    if (run_regular_queue) {
        AbstractQueue<QueueElement> *regular_queue = new RegularPriorityQueue<QueueElement>(empty_element);
        auto f = [start_vertex, regular_queue](const AdjList &graph) {
            return calc_sssp_dijkstra(graph, start_vertex, 1, *regular_queue);
        };
        dijkstra_implementations.emplace_back(f, "RegularQueue");
    }
    for (const auto & param: params) {
        int num_threads = param.first;
        int size_multiple = param.second;
        AbstractQueue<QueueElement> * multi_queue = new MultiQueue<QueueElement>(num_threads, size_multiple, empty_element);
        std::string impl_name = "Multiqueue " + std::to_string(num_threads) + " " + std::to_string(size_multiple);
        dijkstra_implementations.emplace_back([start_vertex, num_threads, multi_queue] (const AdjList & graph)
            { return calc_sssp_dijkstra(graph, start_vertex, num_threads, *multi_queue); }, impl_name);
    }

    read_run_check_write(input_filename_no_ext, dijkstra_implementations);
}
