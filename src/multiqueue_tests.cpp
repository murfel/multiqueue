#include <ostream>

#include "multiqueue.h"

class QueueElement {
private:
    int val, x;
public:
    QueueElement(int val) : val(val), x(0) {}
    bool operator< (const QueueElement & o) const {
        return val < o.val;
    }
    bool operator==(const QueueElement & o) const {
        return val == o.val;
    }
    bool operator!=(const QueueElement & o) const {
        return !operator==(o);
    }
    friend std::ostream & operator<<(std::ostream & out, const QueueElement & element) {
        out << element.val << " " << element.x;
        return out;
    }
};


template <class T>
void producer_routine(Multiqueue<T> & q) {
    for (int i = 0; i < 20; i++) {
        q.push(i);
    }
}

template <class T>
void consumer_routine(Multiqueue<T> & q) {
    for (int i = 0; i < 2; i++) {
        std::cerr << q.pop() << "\n";
    }
}

template <class T>
void test_one_thread() {
    Multiqueue<T> q(10, 1, -1);
    std::cerr << q.pop() << "\n";
    q.push(2);
    q.push(1);
    q.push(3);
    q.push(4);
    std::cerr << q.pop() << "\n";
    std::cerr << q.pop() << "\n";
    std::cerr << q.pop() << "\n";
    std::cerr << q.pop() << "\n";
    q.push(5);
    std::cerr << q.pop() << "\n";
    std::cerr << q.pop() << "\n";
    std::cerr << q.pop() << "\n";
    q.push(2);
    std::cerr << q.pop() << "\n";
    std::cerr << q.pop() << "\n";
}

template <class T>
void test_one_producer_multiple_consumers() {
    Multiqueue<T> q(10, 1, -1);
    std::thread producer(producer_routine<T>, std::ref(q));

    std::vector<std::thread> consumers;
    consumers.reserve(10);
    for (int i = 0; i < 10; i++) {
        consumers.emplace_back(std::thread(consumer_routine<T>, std::ref(q)));
    }
    producer.join();
    for (std::thread & t : consumers) {
        t.join();
    }
}

int main() {
    test_one_thread<int>();
    test_one_thread<QueueElement>();
    test_one_producer_multiple_consumers<int>();
    test_one_producer_multiple_consumers<QueueElement>();
}