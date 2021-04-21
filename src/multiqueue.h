#ifndef MULTIQUEUE_MULTIQUEUE_H
#define MULTIQUEUE_MULTIQUEUE_H

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <iostream>
#include <cstring>
#include <cstdint>
#include <cstdlib>
#include <atomic>

#include "boost/heap/d_ary_heap.hpp"

#include "cached_random.h"

// Set DISTPADDING and QUEUEPADDING to either of padded, aligned, or not_padded.
// If using padded or aligned, set PADDING or ALIGNMENT, respectively.

const std::size_t DUMMY_ITERATION_BEFORE_EXITING = 100'000;

template<class T>
struct padded {
    T first;
    volatile char pad[PADDING]{};
    explicit padded(T && first)
            :first(std::move(first)) { }
};

template<class T>
struct alignas(ALIGNMENT) aligned {
    T first;
};

template<class T>
struct not_padded {
    T first;
};

uint64_t random_fnv1a(uint64_t & seed) {
    const static uint64_t offset = 14695981039346656037ULL;
    const static uint64_t prime = 1099511628211;

    uint64_t hash = offset;
    hash ^= seed;
    hash *= prime;
    seed = hash;
    return hash;
}

template <class T>
class ReservablePriorityQueue : public boost::heap::d_ary_heap<T, boost::heap::arity<8>>
{
public:
    explicit ReservablePriorityQueue(std::size_t reserve_size = 0) {
        this->reserve(reserve_size);
    }
};

template <class T>
class LockablePriorityQueueWithEmptyElement {
private:
    std::atomic<T> top_element{empty_element};
    volatile char my_padding[128]{};
    ReservablePriorityQueue<T> queue;
    T empty_element;
    std::atomic_flag spinlock = ATOMIC_FLAG_INIT;
public:
    LockablePriorityQueueWithEmptyElement() = default;
    LockablePriorityQueueWithEmptyElement(std::size_t reserve_size, const T empty_element) :
            queue(ReservablePriorityQueue<T>(reserve_size)), empty_element(empty_element) {}

    LockablePriorityQueueWithEmptyElement(const LockablePriorityQueueWithEmptyElement & o) = delete;

    LockablePriorityQueueWithEmptyElement(LockablePriorityQueueWithEmptyElement && o) noexcept :
            queue(std::move(o.queue)), empty_element(std::move(o.empty_element)) { }

    LockablePriorityQueueWithEmptyElement & operator=(const LockablePriorityQueueWithEmptyElement & o) = delete;
    LockablePriorityQueueWithEmptyElement & operator=(LockablePriorityQueueWithEmptyElement && o) = delete;

    void push(T value) {
        T old_top = empty_element;
        if (!queue.empty()) {
            old_top = queue.top();
        }
        queue.push(value);
        if (old_top != queue.top()) {
            top_element.store(queue.top());
        }
    }

    const T top() const {
        if (queue.empty()) {
            return empty_element;
        }
        return queue.top();
    }

    const T top_relaxed() const {
        return top_element.load();
    }

    T pop() {
        if (queue.empty()) {
            return empty_element;
        }
        T elem = queue.top();
        queue.pop();
        if (queue.empty()) {
            top_element.store(empty_element);
        } else {
            top_element.store(queue.top());
        }
        return elem;
    }

    std::size_t size() const {
        return queue.size();
    }

    void lock() {
        while (spinlock.test_and_set(std::memory_order_acquire));
    }

    void unlock() {
        spinlock.clear(std::memory_order_release);
    }
};

template<class T>
class Multiqueue {
private:
    std::vector<QUEUEPADDING<LockablePriorityQueueWithEmptyElement<T>>> queues{};
    const std::size_t num_queues;
    const T empty_element;
    std::atomic<std::size_t> num_threads{0};
    std::atomic<std::size_t> num_pushes{0};
    std::vector<std::size_t> max_queue_sizes;

    void push_lock(T value) {
        std::size_t i = gen_random_queue_index();
        auto &queue = queues[i].first;
        queue.lock();
        queue.push(value);
        queue.unlock();
    }

    T pop_lock() {
        for (std::size_t dummy_i = 0; dummy_i < DUMMY_ITERATION_BEFORE_EXITING; dummy_i++) {
            std::size_t i, j;
            i = gen_random_queue_index();
            do {
                j = gen_random_queue_index();
            } while (i == j);

            auto & q1 = queues[std::min(i, j)].first;
            auto & q2 = queues[std::max(i, j)].first;

            T e1 = q1.top_relaxed();
            T e2 = q2.top_relaxed();

            if (e1 == empty_element && e2 == empty_element) {
                continue;
            }

            // reversed comparator because std::priority_queue is a max queue
            if (e1 == empty_element || (e2 != empty_element && e1 < e2)) {
                q2.lock();
                T e = q2.top();
                if (e != e2) {
                    q2.unlock();
                    continue;
                }
                e = q2.pop();
                q2.unlock();
                return e;
            } else {
                q1.lock();
                T e = q1.top();
                if (e != e1) {
                    q1.unlock();
                    continue;
                }
                e = q1.pop();
                q1.unlock();
                return e;
            }
        }
        return empty_element;
    }

public:
    Multiqueue(int num_threads, int size_multiple, T empty_element, std::size_t one_queue_reserve_size) :
            num_queues(num_threads * size_multiple),
            empty_element(empty_element), max_queue_sizes(num_queues, 0) {
        queues.reserve(num_queues);
        for (std::size_t i = 0; i < num_queues; i++) {
            queues.emplace_back(LockablePriorityQueueWithEmptyElement<T>(one_queue_reserve_size, empty_element));
        }
    }
    uint8_t gen_random_queue_index() {
        return cached_random<uint8_t>::next();
    }
    void push(T value) {
        if (num_queues == 1) {
            auto & q = queues.front().first;
            q.lock();
            q.push(value);
            q.unlock();
            return;
        }
        push_lock(value);
    }
    T pop() {
        if (num_queues == 1) {
            auto & q = queues.front().first;
            q.lock();
            T e = q.top();
            q.pop();
            q.unlock();
            return e;
        }
        return pop_lock();
    }
    std::size_t get_num_pushes() const {
        return num_pushes;
    }
    const std::vector<std::size_t> & get_max_queue_sizes() const {
        return max_queue_sizes;
    }
};

#endif //MULTIQUEUE_MULTIQUEUE_H