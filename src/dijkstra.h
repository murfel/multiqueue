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

#include "multiqueue.h"

#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif

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
    virtual void push(T * element, DistType new_dist) = 0;
    virtual std::size_t get_num_pushes() {
        return 0;
    }
    virtual std::vector<std::size_t> get_max_queue_sizes() {
        return std::vector<std::size_t>();
    }
    virtual T * pop() = 0;
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


class MultiQueue : public AbstractQueue<QueueElement> {
private:
    Multiqueue queue;
public:
    MultiQueue(const int num_threads, const int size_multiple, std::size_t one_queue_reserve_size) :
            queue(num_threads, size_multiple, one_queue_reserve_size) {}
    void push(QueueElement * element, DistType new_dist) override {
        queue.push(element, new_dist);
    }
    std::size_t get_num_pushes() override {
        return queue.get_num_pushes();
    }
    std::vector<std::size_t> get_max_queue_sizes() override {
        return queue.get_max_queue_sizes();
    }
    QueueElement * pop() override {
        return queue.pop();
    }
};

using QueueFactory = std::function<std::unique_ptr<AbstractQueue<QueueElement>>()>;

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

void thread_routine(const AdjList & graph, AbstractQueue<QueueElement> & queue, std::vector<QueueElement> & vertexes) {
    while (true) {
        QueueElement * elem = queue.pop();
        // TODO: fix that most treads might exit if one thread is stuck at cut-vertex
        if (elem->dist == EMPTY_ELEMENT_DIST) {
//            std::cerr << "bye" << std::endl;
            break;
        }
        const Vertex v = elem->vertex;
        const DistType & v_dist = elem->dist;
        for (Edge e : graph[v]) {
            Vertex v2 = e.get_to();
            if (v == v2) continue;
            while (true) {
                DistType new_v2_dist = v_dist + e.get_weight();
                DistType old_v2_dist = vertexes[v2].dist;
                if (old_v2_dist <= new_v2_dist) {
                    break;
                }
                queue.push(&vertexes[v2], new_v2_dist);
            }
        }
    }
}

SsspDijkstraDistsAndStatistics calc_sssp_dijkstra(const AdjList & graph, std::size_t num_threads,
                                                  const QueueFactory & queue_factory) {
    const Vertex START_VERTEX = 0;
    std::size_t num_vertexes = graph.size();
    auto queue_ptr = queue_factory();
    AbstractQueue<QueueElement> & queue = *queue_ptr;
    std::vector<QueueElement> vertexes;
    vertexes.reserve(num_vertexes);
    for (std::size_t i = 0; i < num_vertexes; i++) {
        vertexes.emplace_back(i);
    }
    queue.push(&vertexes[START_VERTEX], 0);
    std::vector<std::thread> threads;
    for (std::size_t i = 0; i < num_threads; i++) {
        threads.emplace_back(thread_routine, std::cref(graph), std::ref(queue), std::ref(vertexes));
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
    DistVector dists(num_vertexes);
    for (std::size_t i = 0; i < num_vertexes; i++) {
        dists[i] = vertexes[i].dist;
    }
    return {dists};
}

SsspDijkstraDistsAndStatistics calc_sssp_dijkstra_sequential(const AdjList & graph) {
    const Vertex START_VERTEX = 0;
    std::size_t num_vertexes = graph.size();
    DistVector dists(num_vertexes, INT_MAX);
    std::vector<bool> removed_from_queue(num_vertexes, false);
    std::priority_queue<QueueElement> q;
    dists[START_VERTEX] = 0;
    q.emplace(START_VERTEX, 0);
    for (std::size_t i = 0; i < num_vertexes; i++) {
        while (!q.empty() && removed_from_queue[q.top().vertex]) {
            q.pop();
        }
        if (q.empty()) {
            break;
        }
        Vertex from = q.top().vertex;
        DistType dist = q.top().dist;
        q.pop();
        removed_from_queue[from] = true;
        for (const Edge & edge: graph[from]) {
            Vertex to = edge.get_to();
            DistType new_dist = dist + edge.get_weight();
            if (dists[to] > new_dist) {
                dists[to] = new_dist;
                q.emplace(to, new_dist);
            }
        }
    }
    return {dists};
}