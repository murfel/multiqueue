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

// Set DISTPADDING and QUEUEPADDING to either of padded, aligned, or not_padded.
// If using padded or aligned, set PADDING or ALIGNMENT, respectively.

const std::size_t DUMMY_ITERATION_BEFORE_EXITING = 100;

template<class T>
struct padded {
    T first;
    volatile char pad[PADDING];
};

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
    std::atomic<T> top_element{empty_element};
    volatile char my_padding[128];
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
        T old_top = queue.top();
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
//    QUEUEPADDING<std::atomic<std::size_t>> num_non_empty_queues;
    const T empty_element;
    std::atomic<std::size_t> num_threads{0};
    std::atomic<std::size_t> num_pushes{0};
    std::vector<std::size_t> max_queue_sizes;
    const bool use_try_lock;
    const bool collect_statistics;

    void push_lock(T value) {
        std::size_t i = gen_random_queue_index();
        auto &queue = queues[i].first;
        queue.lock();
//        if (queue.top() == empty_element) {
//            num_non_empty_queues.first++;
//        }
        queue.push(value);
//        if (collect_statistics) {
//            max_queue_sizes[queue] = std::max(max_queue_sizes[queue], queue.size());
//            num_pushes++;
//        }
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
//        if (q.top() == empty_element) {
//            num_non_empty_queues.first++;
//        }
        q.push(value);
//        if (collect_statistics) {
//            max_queue_sizes[i] = std::max(max_queue_sizes[i], q.size());
//            num_pushes++;
//        }
        q.unlock();
    }
    T pop_lock() {
        for (std::size_t dummy_i = 0; dummy_i < DUMMY_ITERATION_BEFORE_EXITING; dummy_i++) {
//            if (num_non_empty_queues.first == 0) {
//                return empty_element;
//            }

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
//                if (q2.top() == empty_element) {
//                    num_non_empty_queues.first--;
//                }
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
//                if (q1.top() == empty_element) {
//                    num_non_empty_queues.first--;
//                }
                e = q1.pop();
                q1.unlock();
                return e;
            }
        }
        return empty_element;
    }

    T pop_try_lock() {
        while (true) {
//            if (num_non_empty_queues.first == 0) {
//                return empty_element;
//            }

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
//            if (q.top() == empty_element) {
//                num_non_empty_queues.first--;
//            }
            q.unlock();
            return e;
        }
    }
public:
    Multiqueue(int num_threads, int size_multiple, T empty_element, std::size_t one_queue_reserve_size) :
            num_queues(num_threads * size_multiple),
            empty_element(empty_element), max_queue_sizes(num_queues, 0),
            use_try_lock(false), collect_statistics(false) {
//        num_non_empty_queues.first = 0;
        queues.reserve(num_queues);
        for (std::size_t i = 0; i < num_queues; i++) {
            QUEUEPADDING<LockablePriorityQueueWithEmptyElement<T>> p;
            p.first = LockablePriorityQueueWithEmptyElement<T>(one_queue_reserve_size, empty_element);
            queues.emplace_back(p);
        }
    }
    std::size_t gen_random_queue_index() {
        thread_local uint64_t seed = 2758756369U + num_threads++;
        return random_fnv1a(seed) % num_queues;
    }
    void push(T value) {
        if (num_queues == 1) {
            auto & q = queues.front().first;
            q.lock();
            q.push(value);
            q.unlock();
            return;
        }
        if (use_try_lock) {
            push_try_lock(value);
        } else {
            push_lock(value);
        }
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