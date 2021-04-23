#ifndef MULTIQUEUE_NUMA_MQ_H
#define MULTIQUEUE_NUMA_MQ_H

#include <cstddef>
#include <cstdint>
#include <vector>

#include "../multiqueue.h"


template<class T>
class numa_mq {
private:
    const std::vector<std::unique_ptr<Multiqueue<T>>> mqs{};

    Multiqueue<T>& get_node_mq(int node_id);
public:
    numa_mq(int num_threads, int size_multiple, T empty_element, std::size_t one_queue_reserve_size);
    void push(T value);
    T pop();
    std::size_t size() const;
    uint16_t get_num_queues() const;
};

#endif //MULTIQUEUE_NUMA_MQ_H
