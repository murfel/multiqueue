#ifndef MULTIQUEUE_SPINLOCK_H
#define MULTIQUEUE_SPINLOCK_H

#include <atomic>

class Spinlock {
private:
    std::atomic_flag spinlock{false};
public:
    void lock() {
//        std::cerr << "locking " << std::endl;
//        spinlock.clear();
        while (spinlock.test_and_set())
            ;
//        std::cerr << "locked!" << std::endl;
    }
    void unlock() {
        spinlock.clear();
    }
};

#endif //MULTIQUEUE_SPINLOCK_H
