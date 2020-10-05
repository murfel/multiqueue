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
#include <unordered_map>

#include "binary_heap.h"

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

uint64_t random_fnv1a(uint64_t & seed) {
    const static uint64_t offset = 14695981039346656037ULL;
    const static uint64_t prime = 1099511628211;

    uint64_t hash = offset;
    hash ^= seed;
    hash *= prime;
    seed = hash;
    return hash;
}

class Multiqueue {
private:
    std::vector<QUEUEPADDING<BinaryHeap>> queues;
    const std::size_t num_queues;
    std::atomic<std::size_t> num_threads{0};
    std::atomic<std::size_t> num_pushes{0};
    std::vector<std::size_t> max_queue_sizes;
    const bool use_try_lock;
    const bool collect_statistics;

    void push_lock(QueueElement * element, int new_dist) {
        // we can change dist only once the corresponding binheap is locked
        while (true) {
            int EMPTY_Q_ID = -1;
//            element->q_id.compare_exchange_strong(EMPTY_Q_ID, gen_random_queue_index());
            int q_id = element->q_id;
            bool adding = false;
            if (q_id == EMPTY_Q_ID) {
                adding = true;
                q_id = gen_random_queue_index();
            }
            auto & queue = queues[q_id].first;
            queue.lock();
            // q_id could:
            // 0) was -1, we generated random id
            // 1) stay the same but dist might change (push)
            // 2) become -1 (pop)
            // 3) change to another queue id (pop, push)
            if (element->q_id == q_id) {
                if (adding) {
                    queue.push(element);
                } else if (new_dist < element->dist) {
                    element->dist = new_dist;
                    queue.decrease_key(element, new_dist);
                }
                queue.unlock();
                break;
            } else if (element->q_id == EMPTY_Q_ID) {
                // someone poped the element, but since we already locked this queue, why not to push to the same queue
                if (!element->q_id.compare_exchange_strong(EMPTY_Q_ID, q_id)) {
                    queue.unlock();
                    continue;
                } else {
                    if (new_dist < element->dist) {
                        element->dist = new_dist;
                        queue.push(element);
                    }
                    queue.unlock();
                    break;
                }
            } else {
                queue.unlock();
                continue;
            }
        }
    }

    QueueElement * pop_lock() {
        for (std::size_t dummy_i = 0; dummy_i < DUMMY_ITERATION_BEFORE_EXITING; dummy_i++) {
            std::size_t i, j;
            i = gen_random_queue_index();
            do {
                j = gen_random_queue_index();
            } while (i == j);

            auto & q1 = queues[std::min(i, j)].first;
            auto & q2 = queues[std::max(i, j)].first;

            q1.lock();
            q2.lock();

            QueueElement * e1 = q1.top();
            QueueElement * e2 = q2.top();

            if (*e1 == EMPTY_ELEMENT && *e2 == EMPTY_ELEMENT) {
                q1.unlock();
                q2.unlock();
                continue;
            }

            // reversed comparator because std::priority_queue is a max queue
            if (*e1 == EMPTY_ELEMENT || (*e2 != EMPTY_ELEMENT && *e1 < *e2)) {
                q1.unlock();
                q2.pop();
                QueueElement * e = e2;
                e->q_id = -1;
                q2.unlock();
                return e;
            } else {
                q2.unlock();
                q1.pop();
                QueueElement * e = e1;
                e->q_id = -1;
                q1.unlock();
                return e;
            }
        }
        return (QueueElement *)&EMPTY_ELEMENT;
    }
public:
    Multiqueue(int num_threads, int size_multiple, std::size_t one_queue_reserve_size) :
            num_queues(num_threads * size_multiple), max_queue_sizes(num_queues, 0),
            use_try_lock(false), collect_statistics(false) {
        queues.reserve(num_queues);
        for (std::size_t i = 0; i < num_queues; i++) {
            QUEUEPADDING<BinaryHeap> p;
            p.first = BinaryHeap(one_queue_reserve_size);
            queues.push_back(p);
        }
    }
    std::size_t gen_random_queue_index() {
        thread_local uint64_t seed = 2758756369U + num_threads++;
        return random_fnv1a(seed) % num_queues;
    }
    void push(QueueElement * element, int new_dist) {
        if (num_queues == 1) {
            auto & q = queues.front().first;
            q.lock();
            q.push(element);
            q.unlock();
            return;
        }
        push_lock(element, new_dist);
    }
    QueueElement * pop() {
        if (num_queues == 1) {
            auto & q = queues.front().first;
            q.lock();
            QueueElement * e = q.top();
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