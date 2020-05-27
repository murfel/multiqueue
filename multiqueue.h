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
};

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
    explicit ReservablePriorityQueue(std::size_t reserve_size) {
        this->c.reserve(reserve_size);
    }
};

template <class T>
class LockablePriorityQueueWithEmptyElement {
private:
    ReservablePriorityQueue<T> queue;
    const T empty_element;
public:
    explicit LockablePriorityQueueWithEmptyElement(std::size_t reserve_size, const T empty_element) :
            queue(ReservablePriorityQueue<T>(reserve_size)), empty_element(empty_element) {}

    LockablePriorityQueueWithEmptyElement(const LockablePriorityQueueWithEmptyElement & o) :
            queue(ReservablePriorityQueue<T>(512)), empty_element(empty_element) {}

    std::mutex mutex;

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
};

template<class T>
class Multiqueue {
private:
    std::vector<LockablePriorityQueueWithEmptyElement<T>> queues;
    const std::size_t num_queues;
    std::atomic<std::size_t> num_non_empty_queues;
    T empty_element;
    std::atomic<std::size_t> num_threads{0};
    std::atomic<std::size_t> num_pushes;
    std::vector<std::size_t> max_queue_sizes;
    bool use_try_lock;

    void push_lock(T value) {
        std::size_t i = gen_random_queue_index();
        auto &queue = queues[i];
        std::lock_guard<std::mutex> lock(queue.mutex);
        if (queue.top() == empty_element) {
            num_non_empty_queues++;
        }
        queue.push(value);
        max_queue_sizes[i] = std::max(max_queue_sizes[i], queue.size());
        num_pushes++;
    }

    void push_try_lock(T value) {
        LockablePriorityQueueWithEmptyElement<T> * q_ptr;
        std::size_t i;
        do {
            i = gen_random_queue_index();
            q_ptr = &queues[i];
        } while (!q_ptr->mutex.try_lock());
        auto & q = *q_ptr;
        if (q.top() == empty_element) {
            num_non_empty_queues++;
        }
        q.push(value);
        max_queue_sizes[i] = std::max(max_queue_sizes[i], q.size());
        q.mutex.unlock();
        num_pushes++;
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

            auto & q1 = queues[std::min(i, j)];
            auto & q2 = queues[std::max(i, j)];

            std::unique_lock<std::mutex> lock1(q1.mutex);
            std::unique_lock<std::mutex> lock2(q2.mutex);

            T e1 = q1.top();
            T e2 = q2.top();

            if (e1 == empty_element && e2 == empty_element) {
                continue;
            }

            LockablePriorityQueueWithEmptyElement<T> * q_ptr;
            // reversed comparator because std::priority_queue is a max queue
            if (e1 == empty_element || (e2 != empty_element && e1 < e2)) {
                lock1.unlock();
                T e = q2.pop();
                if (q2.top() == empty_element) {
                    num_non_empty_queues--;
                }
                return e;
            } else {
                lock2.unlock();
                T e = q1.pop();
                if (q1.top() == empty_element) {
                    num_non_empty_queues--;
                }
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
                q1_ptr = &queues[i];
            } while (!q1_ptr->mutex.try_lock());
            do {
                // lock in the increasing order to avoid the ABA problem
                do {
                    j = gen_random_queue_index();
                } while (j <= i);
                q2_ptr = &queues[j];
            } while (!q2_ptr->mutex.try_lock());

            auto & q1 = *q1_ptr;
            auto & q2 = *q2_ptr;

            T e1 = q1.top();
            T e2 = q2.top();

            if (e1 == empty_element && e2 == empty_element) {
                q1.mutex.unlock();
                q2.mutex.unlock();
                continue;
            }

            LockablePriorityQueueWithEmptyElement<T> * q_ptr;
            // reversed comparator because std::priority_queue is a max queue
            if (e1 == empty_element || (e2 != empty_element && e1 < e2)) {
                q1.mutex.unlock();
                q_ptr = &q2;
            } else {
                q2.mutex.unlock();
                q_ptr = &q1;
            }
            auto & q = *q_ptr;

            T e = q.pop();
            if (q.top() == empty_element) {
                num_non_empty_queues--;
            }
            q.mutex.unlock();
            return e;
        }
    }
public:
    Multiqueue(int num_threads, int size_multiple, T empty_element, std::size_t one_queue_reserve_size = 512,
            bool use_try_lock = false) :
            num_queues(std::max(2, num_threads * size_multiple)), num_non_empty_queues(0),
            empty_element(empty_element), num_pushes(0), max_queue_sizes(num_queues, 0), use_try_lock() {
        queues.reserve(num_queues);
        for (std::size_t i = 0; i < num_queues; i++) {
            queues.emplace_back(one_queue_reserve_size, empty_element);
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