#include <iostream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <queue>
#include <cstring>
#include <fstream>

#include "multiqueue.h"

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
    [[nodiscard]] Vertex get_to() const {
        return to;
    }
    [[nodiscard]] DistType get_weight() const {
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
            queue(Multiqueue(num_threads, size_multiple, empty_element)), empty_element(empty_element) {}
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
    [[nodiscard]] Vertex get_vertex() const {
        return vertex;
    }
    [[nodiscard]] DistType get_dist() const {
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

AtomicDistVector calc_sssp_dijkstra(const AdjList & graph, std::size_t start_vertex, std::size_t num_threads,
                                    AbstractQueue<QueueElement> & queue) {
    queue.push({start_vertex, 0});
    std::vector<std::thread> threads;
    AtomicDistVector dists(graph.size());
    for (std::atomic<DistType> & dist: dists) {
        std::atomic_store(&dist, INT_MAX);
    }
    std::atomic_store(&dists[0], 0);
    for (std::size_t i = 0; i < num_threads; i++) {
        threads.emplace_back(thread_routine, std::cref(graph), std::ref(queue), std::ref(dists));
    }
    for (std::thread & thread : threads) {
        thread.join();
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

AdjList read_edges_into_adj_list(std::istream & istream) {
    std::size_t num_verticies, num_edges;
    istream >> num_verticies >> num_edges;
    AdjList adj_list(num_verticies);
    for (int i = 0; i < num_edges; i++) {
        Vertex from, to;
        DistType weight;
        char c;
        istream >> c >> from >> to >> weight;
        if (weight <= 0) continue;
        adj_list[from].emplace_back(to, weight);
    }
    return adj_list;
}

void write_answer(std::ostream & ostream, const AtomicDistVector & dists) {
    for (const std::atomic<DistType> & dist : dists) {
        ostream << std::atomic_load(&dist) << " ";
    }
    ostream << std::endl;
}

int main(int argc, char *argv[]) {
    std::ifstream input("../graph_n10.txt");
    AdjList graph = read_adj_matrix_into_adj_list(input);
//    AdjList graph = read_edges_into_adj_list(input);
    int num_threads = 1;
//    AbstractQueue<QueueElement> * blocking_queue = new BlockingQueue<QueueElement>({0, -1});
    AbstractQueue<QueueElement> * multi_queue = new MultiQueue<QueueElement>(num_threads, 1, {0, -1});
    AtomicDistVector dists = calc_sssp_dijkstra(graph, 0, num_threads, *multi_queue);
    write_answer(std::cout, dists);
}