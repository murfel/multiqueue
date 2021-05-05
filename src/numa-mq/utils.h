#ifndef MULTIQUEUE_UTILS_H
#define MULTIQUEUE_UTILS_H

#include <thread>
#include <cstddef>

inline void pin_thread_native(std::size_t thread_id, const pthread_t& native_handle) {
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(thread_id, &cpu_set);
    int rc = pthread_setaffinity_np(native_handle, sizeof(cpu_set_t), &cpu_set);
    (void)rc;
}

inline void pin_thread(std::size_t thread_id, std::thread& thread) {
    pin_thread_native(thread_id, thread.native_handle());
}
#endif //MULTIQUEUE_UTILS_H