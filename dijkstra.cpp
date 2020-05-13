#include <iostream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <queue>
#include <cstring>
#include <fstream>
#include <climits>
#include <atomic>

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
    bool operator<(const QueueElement & o) const {
        return dist < o.get_dist();
    }
};

void thread_routine(const AdjList & graph, AbstractQueue<QueueElement> & queue, AtomicDistVector & dists) {
    while (true) {
        QueueElement elem = queue.pop();
        if (elem == queue.get_empty_element()) {
            break;
        }
        Vertex v = elem.get_vertex();
        DistType v_dist = elem.get_dist();
        DistType v_global_dist = std::atomic_load(&dists[v]);
        if (v_dist > v_global_dist) {
            continue;
        }
        for (Edge e : graph[v]) {
            if (v == e.get_to()) continue;
            DistType new_v2_dist = v_dist + e.get_weight();
            while (true) {
                DistType old_v2_dist = std::atomic_load(&dists[e.get_to()]);
                if (old_v2_dist < new_v2_dist) {
                    break;
                }
                if (std::atomic_compare_exchange_weak(&dists[e.get_to()], &old_v2_dist, new_v2_dist)) {
                    queue.push({e.get_to(), new_v2_dist});
                    break;
                }
            }
        }
    }
}

DistVector calc_sssp_dijkstra(const AdjList & graph, std::size_t start_vertex, std::size_t num_threads,
                                    AbstractQueue<QueueElement> & queue) {
    queue.push({start_vertex, 0});
    std::vector<std::thread> threads;
    AtomicDistVector atomic_dists(graph.size());
    for (std::atomic<DistType> & dist: atomic_dists) {
        std::atomic_store(&dist, INT_MAX);
    }
    std::atomic_store(&atomic_dists[0], 0);
    for (std::size_t i = 0; i < num_threads; i++) {
        threads.emplace_back(thread_routine, std::cref(graph), std::ref(queue), std::ref(atomic_dists));
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
    DistVector dists;
    dists.reserve(atomic_dists.size());
    for (const std::atomic<DistType> & x : atomic_dists) {
        dists.push_back(x.load());
    }
    return dists;
}

DistVector calc_sssp_dijkstra_sequential(const AdjList & graph, std::size_t start_vertex) {
    DistVector dists(graph.size(), INT_MAX);
    std::vector<bool> removed_from_queue(graph.size(), false);
    auto comp = [](QueueElement q1, QueueElement q2) { return q1.get_dist() > q2.get_dist(); };
    std::priority_queue<QueueElement, std::vector<QueueElement>, decltype(comp)> q(comp);
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
    return dists;
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
        ostream << dist << " ";
    }
    ostream << std::endl;
}

void read_run_write(const std::string & filename,
        const std::vector<std::function<DistVector(const AdjList &)>> & dijkstra_implementations) {
    auto start = std::chrono::high_resolution_clock::now();
    std::ifstream input("../" + filename + ".in");
    AdjList graph = read_edges_into_adj_list(input, -1);
    auto finish = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = finish - start;
    std::cout << "Reading elapsed time: " << elapsed.count() << " s\n";

    for (std::size_t i = 0; i < dijkstra_implementations.size(); i++) {
        const auto & f = dijkstra_implementations[i];
        start = std::chrono::high_resolution_clock::now();
        DistVector dists = f(graph);
        finish = std::chrono::high_resolution_clock::now();
        elapsed = finish - start;
        std::cout << "Dijkstra implementation #" << i << " elapsed time: " << elapsed.count() << " s\n";
        std::ofstream output("../" + filename + ".out" + std::to_string(i));
        write_answer(output, dists);
    }
}

int main(int argc, char *argv[]) {
    Vertex start_vertex = 0;
    int num_threads = 10;
    QueueElement empty_element = {start_vertex, -1};
    AbstractQueue<QueueElement> * blocking_queue = new BlockingQueue<QueueElement>(empty_element);
    AbstractQueue<QueueElement> * multi_queue = new MultiQueue<QueueElement>(num_threads, 1, empty_element);

    auto f = [start_vertex] (const AdjList & graph)
            { return calc_sssp_dijkstra_sequential(graph, start_vertex); };
    auto f2 = [start_vertex, num_threads, blocking_queue] (const AdjList & graph)
            { return calc_sssp_dijkstra(graph, start_vertex, num_threads, *blocking_queue); };
    auto f3 = [start_vertex, num_threads, multi_queue] (const AdjList & graph)
            { return calc_sssp_dijkstra(graph, start_vertex, num_threads, *multi_queue); };

    std::vector<std::function<DistVector(const AdjList &)>> dijkstra_implementations;
    dijkstra_implementations.emplace_back(f);
    dijkstra_implementations.emplace_back(f2);
    dijkstra_implementations.emplace_back(f3);

    if (argc != 2) {
        std::cerr << "Please pass a filename as an argument." << std::endl;
        exit(1);
    }
    std::string filename(argv[1]);

    read_run_write(filename, dijkstra_implementations);
}
