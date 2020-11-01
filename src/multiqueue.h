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

inline uint64_t random_fnv1a(uint64_t & seed) {
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

    // element->dist should be > new_dist, otherwise nothing happens
    void push_lock(QueueElement * element, int new_dist) {
        // we can change dist only once the corresponding binheap is locked
        while (true) {
            int EMPTY_Q_ID = -1;
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
            // 1) stay the same but dist might or might not change (push OR none)
            // 2) become -1 (pop)
            // 3) change to another queue id (pop, push)
            if (element->q_id == q_id) { // 1
                if (new_dist < element->dist) {
                    element->dist = new_dist;
                    queue.decrease_key(element, new_dist);
                }
                queue.unlock();
                break;
            } else if (adding or element->q_id == EMPTY_Q_ID) {
                // 0, aka element->q_id was EMPTY_ID;
                // OR 2, aka someone popped the element, but since we already locked this queue, push to it
                element->empty_q_id_lock.lock();
                if (element->q_id != EMPTY_Q_ID) {
                    element->empty_q_id_lock.unlock();
                    queue.unlock();
                    continue;
                }
                if (new_dist < element->dist) {
                    element->dist = new_dist;
                    queue.push(element);
                    element->q_id = q_id;
                }
                element->empty_q_id_lock.unlock();
                queue.unlock();
                break;
            } else {  // 3
                queue.unlock();
                continue;
            }
        }
    }

    QueueElement * pop_lock() {
        while (true) {
            bool seen_progress_by_other_threads = false;
            for (std::size_t dummy_i = 0; dummy_i < DUMMY_ITERATION_BEFORE_EXITING; dummy_i++) {
                std::size_t i, j;
                i = gen_random_queue_index();
                do {
                    j = gen_random_queue_index();
                } while (i == j);

                auto &q1 = queues[std::min(i, j)].first;
                auto &q2 = queues[std::max(i, j)].first;

                QueueElement *e1 = q1.top();
                QueueElement *e2 = q2.top();

                if (e1 == nullptr || e2 == nullptr) {
                    seen_progress_by_other_threads = true;
                    break;
                }

                if (*e1 == EMPTY_ELEMENT && *e2 == EMPTY_ELEMENT) {
                    continue;
                }

                auto * q_ptr = &q1;
                QueueElement *e = e1;
                // reversed comparator because std::priority_queue is a max queue
                if (*e1 == EMPTY_ELEMENT || (*e2 != EMPTY_ELEMENT && *e1 < *e2)) {
                    q_ptr = &q2;
                    e = e2;
                }
                auto & q = *q_ptr;
                q.lock();
                if (q.top() != e) {
                    q.unlock();
                    seen_progress_by_other_threads = true;
                    break;
                }
                q.pop();
                e->q_id = -1;
                q.unlock();
                return e;
            }
            if (seen_progress_by_other_threads) {
                continue;
            }
            return (QueueElement *) &EMPTY_ELEMENT;
        }
    }
public:
    Multiqueue(int num_threads, int size_multiple, std::size_t one_queue_reserve_size) :
            num_queues(num_threads * size_multiple) {
        queues.reserve(num_queues);
        for (std::size_t i = 0; i < num_queues; i++) {
            QUEUEPADDING<BinaryHeap> p;
            p.first = BinaryHeap(one_queue_reserve_size);
            queues.push_back(p);
        }
    }
    std::size_t gen_random_queue_index() {
        static std::atomic<size_t> num_threads_registered{0};
        thread_local uint64_t seed = 2758756369U + num_threads_registered++;
        return random_fnv1a(seed) % num_queues;
    }
    void push(QueueElement * element, int new_dist) {
        push_lock(element, new_dist);
    }
    QueueElement * pop() {
        if (num_queues == 1) {
            auto & q = queues.front().first;
            q.lock();
            if (q.empty()) {
                q.unlock();
                return const_cast<QueueElement *>(&EMPTY_ELEMENT);
            }
            QueueElement * e = q.top();
            e->q_id = -1;
            q.pop();
            q.unlock();
            return e;
        }
        return pop_lock();
    }
};

#endif //MULTIQUEUE_MULTIQUEUE_H