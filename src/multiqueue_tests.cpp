#include <iostream>

#include "dijkstra.h"
#include "cached_random.h"



int main() {
    int num_threads = 36;
    int size_multiple = 4;

    numa_mq<QueueElement> mq(num_threads, size_multiple, EMPTY_ELEMENT, 256);

//    cached_random<uint16_t>::next(num_threads * size_multiple, 1'000);

    boost::barrier barrier(2);
    auto t = std::thread([&mq, &barrier, num_threads, size_multiple]{
        barrier.wait();
        cached_random<uint16_t>::next(num_threads * size_multiple, 1'000);
        cached_random_real<double>::next(1, 1'000);
        mq.push({0, 13});
        for (int i = 0; i < 100; i++) {
            QueueElement e = mq.pop();
            std::cout << e.get_dist() << std::endl;
        }
    });
    pin_thread(0, t);
    barrier.wait();
    t.join();

}