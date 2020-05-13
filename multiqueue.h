#ifndef MULTIQUEUE_MULTIQUEUE_H
#define MULTIQUEUE_MULTIQUEUE_H

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <iostream>
#include <random>
#include <cstring>
#include <cstdint>
#include <cstdlib>


template <class T>
class PriorityQueueWithEmptyElement {
private:
    std::priority_queue<T> queue;
    const T empty_element;
public:
    explicit PriorityQueueWithEmptyElement(const T empty_element) : empty_element(empty_element) {}

    void push(T value) {
        queue.push(value);
    }

    const T & top() {
        if (queue.empty()) {
            return empty_element;
        }
        return queue.top();
    }

    T pop() {
        if (queue.empty()) {
            return empty_element;
        }
        T elem = queue.top();
        queue.pop();
        return elem;
    }
};

template<class T>
class Multiqueue {
private:
    std::mutex mutex;
    std::vector<std::unique_ptr<PriorityQueueWithEmptyElement<T>>> queues;
    T empty_element;
public:
    Multiqueue(const int num_threads, const int size_multiple, T empty_element) :
            empty_element(empty_element) {
        // Invariant: There are always at least two queues and the first one is empty.
        std::size_t num_queues = num_threads * size_multiple + 1;
        queues.reserve(num_queues);
        for (std::size_t i = 0; i < num_queues; i++) {
            queues.emplace_back(new PriorityQueueWithEmptyElement<T>(empty_element));
        }
    }
    void push(T value) {
        std::lock_guard<std::mutex> lock(mutex);
        std::uniform_int_distribution<std::mt19937::result_type> dist(1, queues.size() - 1); // []
        std::random_device dev;
        std::mt19937 rng(dev());
        std::size_t i = dist(rng);
        queues[i]->push(value);
    }
    T pop() {
        std::lock_guard<std::mutex> lock(mutex);
        while (true) {
            std::random_device dev;
            std::mt19937 rng(dev());
            std::uniform_int_distribution<std::mt19937::result_type> dist(1, queues.size() - 1); // []
            std::size_t i, j;
            i = dist(rng);
            do {
                j = dist(rng);
            } while (i == j && queues.size() > 2);

            std::size_t min_ind = std::min(i, j);
            std::size_t max_ind = std::max(i, j);
            i = min_ind;
            j = max_ind;
            T val_i = queues[i]->top();
            T val_j = queues[j]->top();
            PriorityQueueWithEmptyElement<T> * to_pop;
            if (val_i == empty_element) {
                to_pop = queues[j].get();
            } else if (val_j == empty_element) {
                to_pop = queues[i].get();
            } else {
                to_pop = queues[val_i < val_j ? i : j].get();
            }

            if (val_j == empty_element && queues.size() > 2) {
                queues.erase(queues.begin() + j);
            }
            if (val_i == empty_element && queues.size() > 2) {
                queues.erase(queues.begin() + i);
            }
            if (val_i == empty_element && val_j == empty_element) {
                if (queues.size() == 2) {
                    return empty_element;
                }
                continue;
            }

            return to_pop->pop();
        }
    }
};

#endif //MULTIQUEUE_MULTIQUEUE_H