#include <iostream>

#include "dijkstra.h"
#include "cached_random.h"

#include "multiqueue.h"


int main() {
    int num_threads = 36;
    int size_multiple = 4;
    pin_thread_native(0, pthread_self());
    numa_mq<QueueElement> mq(num_threads, size_multiple, EMPTY_ELEMENT, 256);

    cached_random<uint16_t>::next(4, 1'000);
    for (int i = 1; i < 10; i++) {
        mq.push({static_cast<Vertex>(i), i});
    }
    for (int i = 0; i < 10; i++) {
        QueueElement e = mq.pop();
        std::cout << "got dist: " << e.get_dist() << std::endl;
    }

    numa_mq<QueueElement> mq2(num_threads, size_multiple, EMPTY_ELEMENT, 256);
    cached_random<uint16_t>::next(4, 1'000);
    mq2.push({17, 17});
    for (int i = 0; i < 10; i++) {
        QueueElement e = mq2.pop();
        std::cout << "got dist: " << e.get_dist() << std::endl;
    }

//    boost::barrier barrier(2);
//    auto t = std::thread([&mq, &barrier, num_threads, size_multiple]{
//        barrier.wait();
//        cached_random<uint16_t>::next(2, 1'000);
//        mq.push({13, 13});
//        std::cerr << "pushed" << std::endl;
//        for (int i = 0; i < 10; i++) {
//            QueueElement e = mq.pop();
//            std::cout << e.get_dist() << std::endl;
//        }
//    });
//    pin_thread(0, t);
//    barrier.wait();
//    t.join();
}