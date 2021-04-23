#include "utils.h"

void pin_thread(std::size_t thread_id, std::thread& thread) {
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(thread_id, &cpu_set);
    int rc = pthread_setaffinity_np(thread.native_handle(), sizeof(cpu_set_t), &cpu_set);
    (void)rc;
}