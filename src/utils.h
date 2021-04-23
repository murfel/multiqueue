#ifndef MULTIQUEUE_UTILS_H
#define MULTIQUEUE_UTILS_H

#include <thread>
#include <cstddef>

void pin_thread(std::size_t thread_id, std::thread& thread);

#endif //MULTIQUEUE_UTILS_H