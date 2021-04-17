#ifndef MULTIQUEUE_DIJKSTRA_H
#define MULTIQUEUE_DIJKSTRA_H

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

#include <boost/thread/barrier.hpp>

#include "multiqueue.h"

#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif

using DistVector = std::vector<DistType>;

class DummyState {
private:
    benchmark::State * state;
    std::chrono::time_point<std::chrono::high_resolution_clock> start;
    bool running = false;
    std::chrono::milliseconds total{0};
public:
    explicit DummyState(benchmark::State * state = nullptr) : state(state) {}
    DummyState(const DummyState & o) : state(o.state) {}
    void PauseTiming() {
        std::cerr << "hi" << std::endl;
        if (state != nullptr) {
            state->PauseTiming();
        } else {
            if (!running) {
                return;
            }
            auto end = std::chrono::high_resolution_clock::now();
            std::cerr << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << std::endl;
            total += std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            running = false;
        }
    }
    void ResumeTiming() {
        if (state != nullptr) {
            state->ResumeTiming();
        } else {
            if (running) {
                return;
            }
            start = std::chrono::high_resolution_clock::now();
            running = true;
        }
    }
    std::chrono::milliseconds get_total() {
        return total;
    }
};

class Edge {
private:
    Vertex to;
    DistType weight;
public:
    Edge(Vertex to, DistType weight) : to(to), weight(weight) {}
    Vertex get_to() const {
        return to;
    }
    void set_to(Vertex new_to) {
        to = new_to;
    }
    DistType get_weight() const {
        return weight;
    }
};

using AdjList = std::vector<std::vector<Edge>>;

class SsspDijkstraDistsAndStatistics {
private:
    DistVector dists;
    DistVector vertex_pulls_counts;
    std::size_t num_pushes{};
    std::vector<std::size_t> max_queue_sizes;
public:
    SsspDijkstraDistsAndStatistics(
            DistVector dists, DistVector vertex_pulls_counts, size_t num_pushes,
            std::vector<std::size_t> max_queue_sizes) :
            dists(std::move(dists)), vertex_pulls_counts(std::move(vertex_pulls_counts)), num_pushes(num_pushes),
            max_queue_sizes(std::move(max_queue_sizes)) {}
    explicit SsspDijkstraDistsAndStatistics(DistVector dists) : dists(std::move(dists)) {};
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

inline void dijkstra_thread_routine(const AdjList & graph, Multiqueue & queue,
                                    std::vector<QueueElement> & vertexes,
                                    const DummyState& state, boost::barrier & barrier, std::size_t thread_id) {
    (void) state;
    (void) thread_id;
    (void) barrier;
//    barrier.wait();
//    if (thread_id == 0) {
//        state.ResumeTiming();
//    }
//    barrier.wait();

    while (true) {
        QueueElement * elem = queue.pop();
        // TODO: fix that most treads might exit if one thread is stuck at cut-vertex
        if (elem == &EMPTY_ELEMENT) {
//            std::cerr << "bye" << std::endl;
            break;
        }
        const Vertex v = elem->vertex;
        for (Edge e : graph[v]) {
            Vertex v2 = e.get_to();
            if (v == v2) continue;
            while (true) {
                DistType new_v2_dist = elem->get_dist_relaxed() + e.get_weight();
                DistType old_v2_dist = vertexes[v2].get_dist_relaxed();
                if (old_v2_dist <= new_v2_dist) {
                    break;
                }
                queue.push(&vertexes[v2], new_v2_dist);
            }
        }
    }
}

inline SsspDijkstraDistsAndStatistics calc_sssp_dijkstra(const AdjList & graph, std::size_t num_threads,
                                                  int size_multiple, std::size_t one_queue_reserve_size,
                                                  DummyState state) {
//    state.PauseTiming();

    const Vertex START_VERTEX = 0;
    std::size_t num_vertexes = graph.size();
    Multiqueue queue = Multiqueue(num_threads, size_multiple, one_queue_reserve_size);
    std::vector<QueueElement> vertexes;
    vertexes.reserve(num_vertexes);
    for (std::size_t i = 0; i < num_vertexes; i++) {
        vertexes.emplace_back(i);
    }
    queue.push_singlethreaded(&vertexes[START_VERTEX], 0);
    std::vector<std::thread> threads;
    boost::barrier barrier(num_threads);
    for (std::size_t thread_id = 0; thread_id < num_threads; thread_id++) {
        threads.emplace_back(dijkstra_thread_routine, std::cref(graph), std::ref(queue), std::ref(vertexes),
                             std::ref(state), std::ref(barrier), thread_id);
#ifdef __linux__
        cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(thread_id, &cpuset);
            int rc = pthread_setaffinity_np(threads.back().native_handle(), sizeof(cpu_set_t), &cpuset);
            (void)rc;
#endif
    }
    for (std::thread & thread : threads) {
        thread.join();
    }
    //state.PauseTiming();
    DistVector dists(num_vertexes);
    for (std::size_t i = 0; i < num_vertexes; i++) {
        dists[i] = vertexes[i].get_dist();
    }
    //state.ResumeTiming();
    return SsspDijkstraDistsAndStatistics(dists);
}

class SimpleQueueElement {
public:
    SimpleQueueElement(Vertex vertex, DistType dist) : vertex(vertex), dist(dist) {}
    Vertex vertex;
    DistType dist;
    bool operator<(const SimpleQueueElement & o) const {
        return dist > o.dist;
    }
};

inline SsspDijkstraDistsAndStatistics calc_sssp_dijkstra_sequential(const AdjList & graph, DummyState state) {
    //state.PauseTiming();
    const Vertex START_VERTEX = 0;
    std::size_t num_vertexes = graph.size();
    DistVector dists(num_vertexes, std::numeric_limits<int>::max());
    std::vector<bool> removed_from_queue(num_vertexes, false);
    std::priority_queue<SimpleQueueElement> q;
    dists[START_VERTEX] = 0;
    q.emplace(START_VERTEX, 0);
    state.ResumeTiming();
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
    return SsspDijkstraDistsAndStatistics(dists);
}

#endif //MULTIQUEUE_DIJKSTRA_H
