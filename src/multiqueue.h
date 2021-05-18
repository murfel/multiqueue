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

// Set DIST_PADDING and QUEUE_PADDING to either of padded, aligned, or not_padded.
// If using padded or aligned, set PADDING or ALIGNMENT, respectively.

const std::size_t dummy_iterations_before_exiting = 100;

template<class T>
struct padded {
    T first;
    volatile char pad[PADDING]{};
    explicit padded(std::size_t reserve_size) : first(my_d_ary_heap<>(reserve_size)) {}
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
    std::vector<QUEUE_PADDING<my_d_ary_heap<>>> queues;
    const std::size_t num_queues;
public:
    Multiqueue(int num_threads, int size_multiple, std::size_t one_queue_reserve_size) :
            num_queues(num_threads * size_multiple) {
        queues.reserve(num_queues);
        for (std::size_t i = 0; i < num_queues; i++) {
            queues.emplace_back(one_queue_reserve_size);
        }
    }
    std::size_t gen_random_queue_index() const {
        static std::atomic<size_t> num_threads_registered{0};
        thread_local uint64_t seed = 2758756369U + num_threads_registered++;
        return random_fnv1a(seed) % num_queues;
    }

    void push_singlethreaded(QueueElement * element, int new_dist) {
        std::size_t q_id = gen_random_queue_index();
        element->set_dist_relaxed(new_dist);
        queues[q_id].first.push(element);
    }

    // element->dist should be > new_dist, otherwise nothing happens
    void push(QueueElement * element, int new_dist) {
        // we can change dist only once the corresponding binary heap is locked
        while (true) {
            int empty_q_id = -1;
            int q_id = element->get_q_id_relaxed();
            bool adding = false;
            if (q_id == empty_q_id) {
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
            if (element->get_q_id_relaxed() == q_id) { // 1 // If so under the queue's lock + mb, this is the real q_id.
                if (new_dist < element->get_dist()) {
                    queue.decrease_key(element, new_dist);
                }
                queue.unlock();
                break;
            } else if (adding or element->get_q_id_relaxed() == empty_q_id) {
                // 0, aka element->q_id was empty_q_id;
                // OR 2, aka someone popped the element, but since we already locked this queue, push to it
                // OR this thread didn't see that q_id was changed to -1,
                //     but now it sees that someone popped from this queue under the queue lock's memory barrier.
                element->empty_q_id_lock();
                if (element->get_q_id_relaxed() != empty_q_id) {
                    // Either someone pushed right before this thread, or this thread didn't see that it was pushed
                    // a long time ago, but now it sees the last q_id assigned under the empty lock's memory barrier.
                    element->empty_q_id_unlock();
                    queue.unlock();
                    continue;
                }
                if (new_dist < element->get_dist()) {
                    element->set_dist_relaxed(new_dist);
                    queue.push(element);
                    element->set_q_id_relaxed(q_id);
                }
                element->empty_q_id_unlock();
                queue.unlock();
                break;
            } else {  // 3
                queue.unlock();
                continue;
            }
        }
    }

    QueueElement * pop() {
        if (num_queues == 1) {
            auto & q = queues.front().first;
            q.lock();
            if (q.empty()) {
                q.unlock();
                return const_cast<QueueElement *>(&empty_element);
            }
            QueueElement * e = q.top();
            e->set_q_id_relaxed(-1);
            q.pop();
            q.unlock();
            return e;
        }

        while (true) {
            bool seen_progress_by_other_threads = false;
            for (std::size_t dummy_i = 0; dummy_i < dummy_iterations_before_exiting; dummy_i++) {
                std::size_t i, j;
                i = gen_random_queue_index();
                do {
                    j = gen_random_queue_index();
                } while (i == j);

                auto &q1 = queues[std::min(i, j)].first;
                auto &q2 = queues[std::max(i, j)].first;

                QueueElement *e1 = q1.top_relaxed();
                QueueElement *e2 = q2.top_relaxed();

                if (e1 == &empty_element && e2 == &empty_element) {
                    continue;
                }

                auto * q_ptr = &q1;
                QueueElement *e = e1;
                // reversed comparator because std::priority_queue is a max queue
                if (e1 == &empty_element || (e2 != &empty_element && *e1 < *e2)) {
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
                e->set_q_id_relaxed(-1);
                q.unlock();
                return e;
            }
            if (seen_progress_by_other_threads) {
                continue;
            }
            return const_cast<QueueElement *>(&empty_element);
        }
    }
};

#endif //MULTIQUEUE_MULTIQUEUE_H