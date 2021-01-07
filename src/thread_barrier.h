#ifndef MULTIQUEUE_THREAD_BARRIER_H
#define MULTIQUEUE_THREAD_BARRIER_H

#include <iostream>
#include <condition_variable>
#include <thread>
#include <chrono>
#include <mutex>

/*
 * Adopted from Boost. See
 * https://www.boost.org/doc/libs/1_74_0/boost/thread/barrier.hpp
 * https://www.boost.org/doc/libs/1_74_0/doc/html/thread/synchronization.html#thread.synchronization.barriers
 * https://stackoverflow.com/a/27118537/3478131
 */
class thread_barrier {
private:
    std::mutex m_mutex;
    std::condition_variable m_cond;
    unsigned int m_count;
    unsigned int m_treshold;
    unsigned int m_generation;
public:
    explicit thread_barrier(unsigned int count) : m_count(count), m_treshold(count), m_generation(0) {}
    void wait() {
        std::unique_lock<std::mutex> lock(m_mutex);
        unsigned int gen = m_generation;
        --m_count;
        if (m_count != 0) {
            m_cond.wait(lock, [gen, this] { return gen != m_generation; });
        } else {
            m_generation++;
            m_count = m_treshold;
            lock.unlock();
            m_cond.notify_all();
        }
    }
};


#endif //MULTIQUEUE_THREAD_BARRIER_H
