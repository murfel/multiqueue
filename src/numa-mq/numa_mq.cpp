#include "numa_mq.h"

#include <numa.h>
#include <netcdf>
#include <thread>

#include "boost/thread/barrier.hpp"

#include "../utils.h"

#define THREADS_PER_NODE 18

template<class T>
numa_mq<T>::numa_mq(int num_threads, int size_multiple, T empty_element, std::size_t one_queue_reserve_size) {
    const int num_nodes = num_threads / THREADS_PER_NODE;
    mqs.resize(num_nodes);
    std::vector<std::thread> threads;
    boost::barrier barrier(num_nodes + 1);
    for (int node_id = 0; node_id < num_nodes; node_id++) {
        threads.emplace_back([&barrier, node_id, this, size_multiple, empty_element, one_queue_reserve_size]() {
                    barrier.wait();
                    assert(sched_getcpu() == node_id == 0 ? 0 : node_id * THREADS_PER_NODE - 1);
                    this->mqs[node_id] = std::make_unique<Multiqueue<T>>(
                            THREADS_PER_NODE, size_multiple, empty_element, one_queue_reserve_size);
                });
        pin_thread(node_id == 0 ? 0 : node_id * THREADS_PER_NODE - 1, threads.back());
    }
    barrier.wait();
    for (auto& thread: threads) {
        thread.join();
    }
}

template<class T>
Multiqueue<T>& numa_mq<T>::get_node_mq(int node_id) {
    return *mqs[node_id];
}

template<class T>
void numa_mq<T>::push(T value) {
    thread_local int node_id = numa_node_of_cpu(sched_getcpu());
    thread_local Multiqueue<T>& mq = get_node_mq(node_id);
    mq.push(value);
}

template<class T>
T numa_mq<T>::pop() {
    thread_local int node_id = numa_node_of_cpu(sched_getcpu());
    thread_local Multiqueue<T>& mq = get_node_mq(node_id);
    return mq.pop();
}

template<class T>
std::size_t numa_mq<T>::size() const {
    std::size_t sum;
    for (auto& mq: mqs) {
        sum += mq.size();
    }
    return sum;
}

template<class T>
uint16_t numa_mq<T>::get_num_queues() const {
    return mqs.size() * mqs.front().get_num_queues();
}