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
#include <pthread.h>
#include <atomic>

// Set DISTPADDING and QUEUEPADDING to either of char_padded, bool_padded, aligned, or not_padded.
// If using char_padded or aligned, set PADDING or ALIGNMENT, respectively.

template<class T, int padding_size = PADDING>
using char_padded = std::pair<T, char[padding_size]>;

template<class T>
using bool_padded = std::pair<T, bool>;

template<class T>
struct alignas(ALIGNMENT) aligned {
    T first;
};

template<class T>
struct not_padded {
    T first;
};

unsigned long xorshf96(unsigned long & x, unsigned long & y, unsigned long & z) { //period 2^96-1
    unsigned long t;
    x ^= x << 16;
    x ^= x >> 5;
    x ^= x << 1;

    t = x;
    x = y;
    y = z;
    z = t ^ x ^ y;

    return z;
}

uint32_t xorshift32(uint32_t & x) {
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    return x;
}

uint32_t xorshift128(uint32_t & a, uint32_t & b, uint32_t & c, uint32_t & d)
{
    /* Algorithm "xor128" from p. 5 of Marsaglia, "Xorshift RNGs" */
    uint32_t t = d;

    uint32_t const s = a;
    d = c;
    c = b;
    b = s;

    t ^= t << 11;
    t ^= t >> 8;
    return a = t ^ s ^ (s >> 19);
}

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
class ReservablePriorityQueue : public std::priority_queue<T, std::vector<T>>
{
public:
    explicit ReservablePriorityQueue(std::size_t reserve_size = 0) {
        this->c.reserve(reserve_size);
    }
};

template <class T>
class LockablePriorityQueueWithEmptyElement {
private:
    ReservablePriorityQueue<T> queue;
    T empty_element;
    std::atomic_flag spinlock = ATOMIC_FLAG_INIT;
public:
    LockablePriorityQueueWithEmptyElement() = default;
    LockablePriorityQueueWithEmptyElement(std::size_t reserve_size, const T empty_element) :
            queue(ReservablePriorityQueue<T>(reserve_size)), empty_element(empty_element) {}

    LockablePriorityQueueWithEmptyElement(const LockablePriorityQueueWithEmptyElement & o) :
            queue(ReservablePriorityQueue<T>(512)), empty_element(o.empty_element) {}

    LockablePriorityQueueWithEmptyElement & operator=(const LockablePriorityQueueWithEmptyElement & o) {
        queue = o.queue;
        empty_element = o.empty_element;
        return *this;
    }

    void push(T value) {
        queue.push(value);
    }

    const T & top() const {
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

    std::size_t size() const {
        return queue.size();
    }

    void lock() {
        while (spinlock.test_and_set(std::memory_order_acquire));
    }

    bool try_lock() {
        while (spinlock.test_and_set(std::memory_order_acquire));
        return true;
    }

    void unlock() {
        spinlock.clear(std::memory_order_release);
    }
};

template<class T>
class Multiqueue {
private:
    std::vector<QUEUEPADDING<LockablePriorityQueueWithEmptyElement<T>>> queues;
    const std::size_t num_queues;
    std::atomic<std::size_t> num_non_empty_queues;
    T empty_element;
    std::atomic<std::size_t> num_threads{0};
    std::atomic<std::size_t> num_pushes;
    std::vector<std::size_t> max_queue_sizes;
    bool use_try_lock;
    bool collect_statistics;

    void push_lock(T value) {
        std::size_t i = gen_random_queue_index();
        auto &queue = queues[i].first;
        queue.lock();
        if (queue.top() == empty_element) {
            num_non_empty_queues++;
        }
        queue.push(value);
        if (collect_statistics) {
            max_queue_sizes[i] = std::max(max_queue_sizes[i], queue.size());
            num_pushes++;
        }
        queue.unlock();
    }

    void push_try_lock(T value) {
        LockablePriorityQueueWithEmptyElement<T> * q_ptr;
        std::size_t i;
        do {
            i = gen_random_queue_index();
            q_ptr = &queues[i].first;
        } while (!q_ptr->try_lock());
        auto & q = *q_ptr;
        if (q.top() == empty_element) {
            num_non_empty_queues++;
        }
        q.push(value);
        if (collect_statistics) {
            max_queue_sizes[i] = std::max(max_queue_sizes[i], q.size());
            num_pushes++;
        }
        q.unlock();
    }
    T pop_lock() {
        while (true) {
            if (num_non_empty_queues == 0) {
                return empty_element;
            }

            std::size_t i, j;
            i = gen_random_queue_index();
            do {
                j = gen_random_queue_index();
            } while (i == j);

            auto & q1 = queues[std::min(i, j)].first;
            auto & q2 = queues[std::max(i, j)].first;

            q1.lock();
            q2.lock();

            T e1 = q1.top();
            T e2 = q2.top();

            if (e1 == empty_element && e2 == empty_element) {
                q1.unlock();
                q2.unlock();
                continue;
            }

            // reversed comparator because std::priority_queue is a max queue
            if (e1 == empty_element || (e2 != empty_element && e1 < e2)) {
                q1.unlock();
                T e = q2.pop();
                if (q2.top() == empty_element) {
                    num_non_empty_queues--;
                }
                q2.unlock();
                return e;
            } else {
                q2.unlock();
                T e = q1.pop();
                if (q1.top() == empty_element) {
                    num_non_empty_queues--;
                }
                q1.unlock();
                return e;
            }
        }
    }

    T pop_try_lock() {
        while (true) {
            if (num_non_empty_queues == 0) {
                return empty_element;
            }

            LockablePriorityQueueWithEmptyElement<T> * q1_ptr;
            LockablePriorityQueueWithEmptyElement<T> * q2_ptr;
            std::size_t i, j;
            do {
                do {
                    i = gen_random_queue_index();
                } while (i == num_queues - 1);
                q1_ptr = &queues[i].first;
            } while (!q1_ptr->try_lock());
            do {
                // lock in the increasing order to avoid the ABA problem
                do {
                    j = gen_random_queue_index();
                } while (j <= i);
                q2_ptr = &queues[j].first;
            } while (!q2_ptr->try_lock());

            auto & q1 = *q1_ptr;
            auto & q2 = *q2_ptr;

            T e1 = q1.top();
            T e2 = q2.top();

            if (e1 == empty_element && e2 == empty_element) {
                q1.unlock();
                q2.unlock();
                continue;
            }

            LockablePriorityQueueWithEmptyElement<T> * q_ptr;
            // reversed comparator because std::priority_queue is a max queue
            if (e1 == empty_element || (e2 != empty_element && e1 < e2)) {
                q1.unlock();
                q_ptr = &q2;
            } else {
                q2.unlock();
                q_ptr = &q1;
            }
            auto & q = *q_ptr;

            T e = q.pop();
            if (q.top() == empty_element) {
                num_non_empty_queues--;
            }
            q.unlock();
            return e;
        }
    }
public:
    Multiqueue(int num_threads, int size_multiple, T empty_element, std::size_t one_queue_reserve_size,
            bool use_try_lock, bool collect_statistics) :
            num_queues(std::max(2, num_threads * size_multiple)), num_non_empty_queues(0),
            empty_element(empty_element), num_pushes(0), max_queue_sizes(num_queues, 0),
            use_try_lock(use_try_lock), collect_statistics(collect_statistics) {
        queues.reserve(num_queues);
        for (std::size_t i = 0; i < num_queues; i++) {
            auto q = LockablePriorityQueueWithEmptyElement<T>(one_queue_reserve_size, empty_element);

            QUEUEPADDING<LockablePriorityQueueWithEmptyElement<T>> p;
            p.first = LockablePriorityQueueWithEmptyElement<T>(one_queue_reserve_size, empty_element);

            queues.push_back(p);
        }
    }
    std::size_t gen_random_queue_index() {
        thread_local uint64_t seed = 2758756369U + num_threads++;
        return random_fnv1a(seed) % num_queues;
    }
    void push(T value) {
        if (use_try_lock) {
            push_try_lock(value);
        } else {
            push_lock(value);
        }
    }
    T pop() {
        if (use_try_lock)  {
            return pop_try_lock();
        } else {
            return pop_lock();
        }
    }
    std::size_t get_num_pushes() const {
        return num_pushes;
    }
    const std::vector<std::size_t> & get_max_queue_sizes() const {
        return max_queue_sizes;
    }
};

#endif //MULTIQUEUE_MULTIQUEUE_H