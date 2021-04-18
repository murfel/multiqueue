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
    void pause_timing() {
        if (state != nullptr) {
            state->PauseTiming();
        } else {
            if (!running) {
                return;
            }
            auto end = std::chrono::high_resolution_clock::now();
            total += std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            running = false;
        }
    }
    void resume_timing() {
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

class DistsAndStatistics {
private:
    DistVector dists;
    DistVector vertex_pulls_counts;
    std::size_t num_pushes{};
    std::vector<std::size_t> max_queue_sizes;
public:
    DistsAndStatistics(
            DistVector dists, DistVector vertex_pulls_counts, size_t num_pushes,
            std::vector<std::size_t> max_queue_sizes) :
            dists(std::move(dists)), vertex_pulls_counts(std::move(vertex_pulls_counts)), num_pushes(num_pushes),
            max_queue_sizes(std::move(max_queue_sizes)) {}
    explicit DistsAndStatistics(DistVector dists) :dists(std::move(dists)) {};
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
};

inline void dijkstra_thread_routine(const AdjList & graph, Multiqueue & queue,
                                    std::vector<QueueElement> & vertexes,
                                    DummyState& state, boost::barrier & barrier, std::size_t thread_id) {
    barrier.wait();
    if (thread_id == 0) {
        state.resume_timing();
    }
    barrier.wait();

    while (true) {
        QueueElement * elem = queue.pop();
        // TODO: fix that most treads might exit if one thread is stuck at cut-vertex
        if (elem == &empty_element) {
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

    barrier.wait();
    if (thread_id == 0) {
        state.pause_timing();
    }
    barrier.wait();
}

inline DistsAndStatistics calc_dijkstra(const AdjList & graph, std::size_t num_threads,
                                                  int size_multiple, std::size_t one_queue_reserve_size,
                                                  DummyState& state) {
    const Vertex start_vertex = 0;
    std::size_t num_vertexes = graph.size();
    Multiqueue queue = Multiqueue(num_threads, size_multiple, one_queue_reserve_size);
    std::vector<QueueElement> vertexes;
    vertexes.reserve(num_vertexes);
    for (std::size_t i = 0; i < num_vertexes; i++) {
        vertexes.emplace_back(i);
    }
    queue.push_singlethreaded(&vertexes[start_vertex], 0);
    std::vector<std::thread> threads;
    boost::barrier barrier(num_threads);
    for (std::size_t thread_id = 0; thread_id < num_threads; thread_id++) {
        threads.emplace_back(dijkstra_thread_routine, std::cref(graph), std::ref(queue), std::ref(vertexes),
                             std::ref(state), std::ref(barrier), thread_id);
#ifdef __linux__
        cpu_set_t cpu_set;
            CPU_ZERO(&cpu_set);
            CPU_SET(thread_id, &cpu_set);
            int rc = pthread_setaffinity_np(threads.back().native_handle(), sizeof(cpu_set_t), &cpu_set);
            (void)rc;
#endif
    }
    for (std::thread & thread : threads) {
        thread.join();
    }
    DistVector dists(num_vertexes);
    for (std::size_t i = 0; i < num_vertexes; i++) {
        dists[i] = vertexes[i].get_dist();
    }
    return DistsAndStatistics(dists);
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

inline DistsAndStatistics calc_dijkstra_sequential(const AdjList & graph, DummyState& state) {
    const Vertex start_vertex = 0;
    std::size_t num_vertexes = graph.size();
    DistVector dists(num_vertexes, std::numeric_limits<int>::max());
    std::vector<bool> removed_from_queue(num_vertexes, false);
    std::priority_queue<SimpleQueueElement> q;
    dists[start_vertex] = 0;
    q.emplace(start_vertex, 0);
    state.resume_timing();
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
    state.pause_timing();
    return DistsAndStatistics(dists);
}

#endif //MULTIQUEUE_DIJKSTRA_H
