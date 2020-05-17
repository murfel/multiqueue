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
class LockablePriorityQueueWithEmptyElement {
private:
    std::priority_queue<T> queue;
    const T empty_element;
public:
    explicit LockablePriorityQueueWithEmptyElement(const T empty_element) : empty_element(empty_element) {}

    LockablePriorityQueueWithEmptyElement(const LockablePriorityQueueWithEmptyElement & o) :
        empty_element(o.empty_element) {}

    std::mutex mutex;

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
    std::vector<LockablePriorityQueueWithEmptyElement<T>> queues;
    std::size_t num_non_empty_queues = 0;
    std::mutex num_non_empty_queues_mutex;
    T empty_element;
    std::random_device dev;
    std::mt19937 rng;
    std::uniform_int_distribution<std::mt19937::result_type> dist; // []
    std::size_t gen_random_queue_index() {
        return dist(rng);
    }
public:
    Multiqueue(int num_threads, int size_multiple, T empty_element) :
            empty_element(empty_element), rng(dev()), dist(0, std::max(2, num_threads * size_multiple) - 1) {
        std::size_t num_queues = std::max(2, num_threads * size_multiple);
        queues.reserve(num_queues);
        for (std::size_t i = 0; i < num_queues; i++) {
            queues.emplace_back(empty_element);
        }
    }
    void push(T value) {
        auto & queue = queues[gen_random_queue_index()];
        std::lock_guard<std::mutex> lock(queue.mutex);
        if (queue.top() == empty_element) {
            std::lock_guard<std::mutex> num_lock(num_non_empty_queues_mutex);
            num_non_empty_queues++;
        }
        queue.push(value);
    }
    T pop() {
        while (true) {
            {
                std::lock_guard<std::mutex> lock(num_non_empty_queues_mutex);
                if (num_non_empty_queues == 0) {
                    return empty_element;
                }
            }

            std::size_t i, j;
            i = gen_random_queue_index();
            do {
                j = gen_random_queue_index();
            } while (i == j);

            auto & q1 = queues[std::min(i, j)];
            auto & q2 = queues[std::max(i, j)];

            std::unique_lock<std::mutex> lock1(q1.mutex);
            std::unique_lock<std::mutex> lock2(q2.mutex);

            T e1 = q1.top();
            T e2 = q2.top();

            if (e1 == empty_element && e2 == empty_element) {
                continue;
            }

            if (e1 == empty_element || (e2 != empty_element && e2 < e1)) {
                lock1.unlock();
                T e = q2.pop();
                if (q2.top() == empty_element) {
                    std::lock_guard<std::mutex> lock(num_non_empty_queues_mutex);
                    num_non_empty_queues--;
                }
                return e;
            } else {
                lock2.unlock();
                T e = q1.pop();
                if (q1.top() == empty_element) {
                    std::lock_guard<std::mutex> lock(num_non_empty_queues_mutex);
                    num_non_empty_queues--;
                }
                return e;
            }
        }
    }
};

#endif //MULTIQUEUE_MULTIQUEUE_H