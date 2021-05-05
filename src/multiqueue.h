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
#include "../spinlock.h"

// Set DISTPADDING and QUEUEPADDING to either of padded, aligned, or not_padded.
// If using padded or aligned, set PADDING or ALIGNMENT, respectively.

const std::size_t DUMMY_ITERATION_BEFORE_EXITING = 1000;

using RandomUintSize = uint16_t;

template<class T>
struct padded {
    T first;
    volatile char pad[PADDING]{};
    explicit padded(T && first)
            :first(std::move(first)) { }
//    ~padded() {
//        std::cerr << "padded d-tor" << std::endl;
//    }
};

template<class T>
struct alignas(ALIGNMENT) aligned {
    T first;
};

template<class T>
struct not_padded {
    T first;
};

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
    Spinlock spinlock;
public:
    LockablePriorityQueueWithEmptyElement() = delete;
    ~LockablePriorityQueueWithEmptyElement() {
        std::cerr << "PQ d-tor" << std::endl;
    }
    LockablePriorityQueueWithEmptyElement(std::size_t reserve_size, const T empty_element) :
            queue(ReservablePriorityQueue<T>(reserve_size)), empty_element(empty_element) {
        std::cerr << "PQ c-tor" << std::endl;
    }

    LockablePriorityQueueWithEmptyElement(const LockablePriorityQueueWithEmptyElement & o) = delete;

    LockablePriorityQueueWithEmptyElement(LockablePriorityQueueWithEmptyElement && o) noexcept :
            queue(std::move(o.queue)), empty_element(std::move(o.empty_element)) {
        std::cerr << "PQ move c-tor" << std::endl;
    }

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
        spinlock.lock();
    }

    void unlock() {
        spinlock.unlock();
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
        std::cerr << "got q " << i << std::endl;
        std::cerr << queue.size() << std::endl;
        queue.lock();
        std::cerr << "q locked" << std::endl;
        std::cerr << queue.size() << std::endl;
        queue.push(value);
        std::cerr << "val pushed" << std::endl;
        queue.unlock();
        std::cerr << "q unlocked" << std::endl;
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

            T e1 = q1.top();
            T e2 = q2.top();

            if (e1 == empty_element && e2 == empty_element) {
                continue;
            }

            if (e1 == empty_element) {
                q2.lock();
                T e = q2.pop();
                if (e == empty_element) {
                    q2.unlock();
                    continue;
                }
                q2.unlock();
                return e;
            }

            if (e2 == empty_element) {
                q1.lock();
                T e = q1.pop();
                if (e == empty_element) {
                    q1.unlock();
                    continue;
                }
                q1.unlock();
                return e;
            }

            auto &q = e1 < e2 ? q2 : q1;

            q.lock();
            T e = q.pop();
            q.unlock();
            return e;


            // reversed comparator because std::priority_queue is a max queue
//            if (e1 == empty_element || (e2 != empty_element && e1 < e2)) {
//                q2.lock();
//                T e = q2.top();
//                if (e != e2) {
//                    q2.unlock();
//                    continue;
//                }
//                e = q2.pop();
//                q2.unlock();
//                return e;
//            } else {
//                q1.lock();
//                T e = q1.top();
//                if (e != e1) {
//                    q1.unlock();
//                    continue;
//                }
//                e = q1.pop();
//                q1.unlock();
//                return e;
//            }
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
        std::cerr << "MQ c-tor finished" << std::endl;
    }
    ~Multiqueue() {
        std::cerr << "MQ d-tor" << std::endl;
    }
    RandomUintSize gen_random_queue_index() {
        return cached_random<RandomUintSize>::next();
    }
    void push(T value) {
        std::cerr << "pushing" << std::endl;
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
    std::size_t size() const {
        std::size_t sum = 0;
        for (auto & q: queues) {
            sum += q.first.size();
        }
        return sum;
    }
    uint16_t get_num_queues() const {
        return num_queues;
    }
};

#endif //MULTIQUEUE_MULTIQUEUE_H