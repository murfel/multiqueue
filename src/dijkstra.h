#include <iostream>
#include <random>
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

#include <benchmark/benchmark.h>

#include "multiqueue.h"


#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif

using Vertex = uint32_t;
using DistType = int;
using DistVector = std::vector<DistType>;
using AtomicDistVector = std::vector<DISTPADDING<std::atomic<DistType>>>;

class Edge {
private:
    Vertex to;
    DistType weight;
public:
    Edge(Vertex to, DistType weight) : to(to), weight(weight) {}
    Vertex get_to() const {
        return to;
    }
    void set_to(Vertex to) {
        this->to = to;
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
    MultiQueue(const int num_threads, const int size_multiple, T empty_element, std::size_t one_queue_reserve_size) :
            queue(num_threads, size_multiple, empty_element, one_queue_reserve_size), empty_element(empty_element) {}
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
    QueueElement(Vertex vertex = 0, DistType dist = -1) : vertex(vertex), dist(dist) {}
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
    SsspDijkstraDistsAndStatistics(DistVector dists) : dists(std::move(dists)) {};
    SsspDijkstraDistsAndStatistics() = default;
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
        DistType v_global_dist = dists[v].first;
        if (v_dist > v_global_dist) {
            continue;
        }
        if (collect_statistics) {
            vertex_pull_counts[v].first++;
        }
        for (Edge e : graph[v]) {
            Vertex v2 = e.get_to();
            if (v == v2) continue;
            DistType new_v2_dist = v_dist + e.get_weight();
            while (true) {
                DistType old_v2_dist = dists[v2].first;
                if (old_v2_dist <= new_v2_dist) {
                    break;
                }
                if (dists[v2].first.compare_exchange_strong(old_v2_dist, new_v2_dist)) {
                    queue.push({v2, new_v2_dist});
                    break;
                }
            }
        }
    }
}

AtomicDistVector initialize_atomic_vector(std::size_t n, DistType x) {
    AtomicDistVector atomic_vector(n);
    for (auto & atomic_element : atomic_vector) {
        atomic_element.first = x;
    }
    return atomic_vector;
}

std::vector<DistType> unwrap_vector_from_atomic(const AtomicDistVector & atomic_vector) {
    std::vector<DistType> regular_vector;
    regular_vector.reserve(atomic_vector.size());
    for (const auto & atomic_element : atomic_vector) {
        regular_vector.push_back(atomic_element.first);
    }
    return regular_vector;
}

SsspDijkstraDistsAndStatistics calc_sssp_dijkstra(const AdjList & graph, std::size_t num_threads,
        const QueueFactory & queue_factory, Vertex start_vertex = 0) {
    std::size_t num_vertexes = graph.size();
    auto queue_ptr = queue_factory();
    AbstractQueue<QueueElement> & queue = *queue_ptr;
    queue.push({start_vertex, 0});
    AtomicDistVector atomic_dists = initialize_atomic_vector(num_vertexes, INT_MAX);
    atomic_dists[0].first = 0;
    std::vector<std::thread> threads;
    for (std::size_t i = 0; i < num_threads; i++) {
        threads.emplace_back(thread_routine, std::cref(graph), std::ref(queue), std::ref(atomic_dists),
                std::ref(atomic_dists), false);
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
    DistVector dists = unwrap_vector_from_atomic(atomic_dists);
    return {dists};
}

SsspDijkstraDistsAndStatistics calc_sssp_dijkstra_sequential(const AdjList & graph, Vertex start_vertex = 0) {
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
    return {dists};
}