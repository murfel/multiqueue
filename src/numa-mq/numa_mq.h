#ifndef MULTIQUEUE_NUMA_MQ_H
#define MULTIQUEUE_NUMA_MQ_H

#include <cstddef>
#include <cstdint>
#include <vector>

#include <numa.h>
#include <netcdf>
#include <thread>

#include "boost/thread/barrier.hpp"

#include "../utils.h"

#define THREADS_PER_NODE 12

#include "../multiqueue.h"

template<class T>
class numa_mq {
private:
    std::vector<Multiqueue<T>*> mqs;
    std::mutex io_mutex;

    Multiqueue<T>& get_node_mq(int node_id) {
        return *mqs[node_id];
    }
public:
    numa_mq(int num_threads, int size_multiple, T empty_element, std::size_t one_queue_reserve_size) {
        const int num_nodes = num_threads / THREADS_PER_NODE + (num_threads % THREADS_PER_NODE != 0);
        mqs.resize(num_nodes);
        std::vector<std::thread> threads;
        boost::barrier barrier(num_nodes + 1);
        size_multiple = 1;
        for (int node_id = 0; node_id < num_nodes; node_id++) {
            threads.emplace_back([&barrier, node_id, this, size_multiple, empty_element, one_queue_reserve_size]() {
                barrier.wait();
                assert(numa_node_of_cpu(sched_getcpu()) == node_id);
                this->mqs[node_id] = new Multiqueue<T>(
                        THREADS_PER_NODE, size_multiple, empty_element, one_queue_reserve_size);
                cached_random<uint16_t>::next(THREADS_PER_NODE * size_multiple, 100);
                this->mqs[node_id]->push({static_cast<uint32_t>(node_id), node_id});
            });
            pin_thread(node_id * THREADS_PER_NODE, threads.back());
        }
        barrier.wait();
        for (auto& thread: threads) {
            thread.join();
        }
        std::cerr << "done init numa mq with nodes: " << num_nodes << ", threads total " << num_threads << std::endl;
        std::cerr << "threads " << num_threads << std::endl;
    }
    void push(T value) {
        thread_local int thread_id = sched_getcpu();
        thread_local int node_id = numa_node_of_cpu(thread_id) % mqs.size();
        thread_local Multiqueue<T>& mq = get_node_mq(node_id);
        mq.push(value);
    }
    T pop() {
        thread_local int thread_id = sched_getcpu();
        thread_local int node_id = numa_node_of_cpu(thread_id);
        thread_local Multiqueue<T>& mq = get_node_mq(node_id);
        std::cerr << thread_id << " " << node_id << std::endl;
        return mq.pop();
    }
    std::size_t size() const {
        std::size_t sum = 0;
        for (const auto& mq: mqs) {
            sum += mq->size();
        }
        return sum;
    }
    uint16_t get_num_queues() {
        return 36;
    }
    ~numa_mq() {
        std::cerr << "numa_mq d-tor" << std::endl;
        for (Multiqueue<T>* mq_ptr: mqs) {
            delete mq_ptr;
        }
    }
};

#endif //MULTIQUEUE_NUMA_MQ_H
