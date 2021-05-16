#include <iostream>
#include <random>
#include <thread>
#include <mutex>
#include <algorithm>
#include <queue>
#include <cstring>
#include <fstream>
#include <limits>
#include <atomic>
#include <functional>
#include <utility>
#include <numeric>
#include <cmath>

#include <benchmark/benchmark.h>

#include "multiqueue.h"
#include "numa-mq/numa_mq.h"
#include "cached_random.h"
#include "cached_random_real.h"
#include "timer.h"
#include "utils.h"

#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif

const int SEQUENTIAL_ITERATIONS = 100;

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

template<class M>
using QueueFactory = std::function<std::unique_ptr<M>()>;

static const DistType EMPTY_ELEMENT_DIST = -1;
static const QueueElement EMPTY_ELEMENT = {0, EMPTY_ELEMENT_DIST};

class DistsAndStatistics {
private:
    DistVector dists;
    DistVector vertex_pulls_counts;
    std::size_t num_pushes{};
    std::vector<std::size_t> max_queue_sizes;
    std::vector<QueueElement> leftover_elements;
public:
    DistsAndStatistics(
            DistVector dists, DistVector vertex_pulls_counts, size_t num_pushes,
            std::vector<std::size_t> max_queue_sizes) :
            dists(std::move(dists)), vertex_pulls_counts(std::move(vertex_pulls_counts)), num_pushes(num_pushes),
            max_queue_sizes(std::move(max_queue_sizes)) {}
    DistsAndStatistics(DistVector dists) :dists(std::move(dists)) {};
    DistsAndStatistics(DistVector dists, std::vector<QueueElement> leftover_elements) :
            dists(std::move(dists)), leftover_elements(std::move(leftover_elements)) {};
    DistsAndStatistics() = default;
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
    const std::vector<QueueElement> &get_leftover_elements() const {
        return leftover_elements;
    }
};

class SimpleQueueElement {
public:
    SimpleQueueElement(Vertex vertex, DistType dist) : vertex(vertex), dist(dist) {}
    Vertex vertex;
    DistType dist;
    bool operator<(const SimpleQueueElement & o) const {
        return dist > o.dist;
    }
};

DistsAndStatistics calc_sssp_dijkstra_sequential(const AdjList & graph, Vertex start_vertex, timer& timer, int iterations = -1) {
    int num_vertexes = (int)graph.size();
    DistVector dists(num_vertexes, std::numeric_limits<DistType>::max());
    std::vector<bool> removed_from_queue(num_vertexes, false);
    std::priority_queue<SimpleQueueElement> q;
    dists[start_vertex] = 0;
    q.push({start_vertex, 0});
    if (iterations == -1) {
        iterations = num_vertexes;
    }
    for (int i = 0; i < iterations; i++) {
        if (i == SEQUENTIAL_ITERATIONS) {
            timer.resume_timing();
        }
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
                q.push({to, new_dist});
            }
        }
    }
    timer.pause_timing();
    if (iterations != num_vertexes) {
        std::vector<QueueElement> leftover_elements;
        while (!q.empty()) {
            if (!removed_from_queue[q.top().vertex]) {
                leftover_elements.emplace_back(q.top().vertex, q.top().dist);
            }
            q.pop();
        }
        return {dists, leftover_elements};
    }
    return {dists};
}

template<class M>
void thread_routine(const AdjList & graph, M & queue, AtomicDistVector & dists,
        AtomicDistVector & vertex_pull_counts, bool collect_statistics, int num_threads, int size_multiple,
        timer& timer, int thread_id, boost::barrier& barrier) {

    cached_random<RandomUintSize>::next(num_threads * size_multiple, 1'000);
    cached_random_real<double>::next(1, 1'000);

    barrier.wait();
    if (thread_id == 0) {
        timer.resume_timing();
    }
    barrier.wait();

    while (true) {
        QueueElement elem = queue.pop();
        // TODO: fix that most treads might exit if one thread is stuck at cut-vertex
        if (elem.get_dist() == EMPTY_ELEMENT_DIST) {
            if (cached_random_real<double>::next() < 0) {
                break;
            } else {
                if (queue.size() == 0) {
                    break;
                } else {
                    continue;
                }
            }
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

    barrier.wait();
    if (thread_id == 0) {
        timer.pause_timing();
    }
    barrier.wait();
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

template<class M>
DistsAndStatistics calc_sssp_dijkstra(const AdjList & graph, std::size_t num_threads,
        const QueueFactory<M> & queue_factory, Vertex start_vertex, timer& timer, int size_multiple,
        int seq_iterations = 0) {
    std::size_t num_vertexes = graph.size();
    auto queue_ptr = queue_factory();
    cached_random<RandomUintSize>::next(THREADS_PER_NODE * size_multiple, 100'000'000);
    cached_random_real<double>::next(1, 1'000);
    M & queue = *queue_ptr;
    AtomicDistVector atomic_dists = initialize_atomic_vector(num_vertexes, std::numeric_limits<int>::max());
    // init queue
    seq_iterations = SEQUENTIAL_ITERATIONS;
    if (seq_iterations == 0) {
        queue.push({start_vertex, 0});
        atomic_dists[start_vertex].first = 0;
    } else {
        class timer t;
        DistsAndStatistics das = calc_sssp_dijkstra_sequential(graph, start_vertex, t, seq_iterations);
        for (int i = 0; i < (int)das.get_dists().size(); i++) {
            atomic_dists[i].first = das.get_dists()[i];
        }
        for (QueueElement e: das.get_leftover_elements()) {
            queue.push(e);
        }
    }
    // finish init
    std::vector<std::thread> threads;
    boost::barrier barrier(num_threads);
    for (std::size_t i = 0; i < num_threads; i++) {
        threads.emplace_back(thread_routine<M>, std::cref(graph), std::ref(queue), std::ref(atomic_dists),
                std::ref(atomic_dists), false, THREADS_PER_NODE, size_multiple, std::ref(timer), i,
                std::ref(barrier));
        pin_thread(i, threads.back());
    }
    for (std::thread & thread : threads) {
        thread.join();
    }
    std::size_t sz = queue.size();
    if (sz != 0) {
        std::cerr << "Warning: Multiqueue.size = " << queue.size() << std::endl;
    }
    DistVector dists = unwrap_vector_from_atomic(atomic_dists);
    return {dists};
}