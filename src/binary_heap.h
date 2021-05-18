#ifndef MULTIQUEUE_BINARY_HEAP_H
#define MULTIQUEUE_BINARY_HEAP_H

#include <atomic>
#include <unordered_map>
#include <vector>
#include <limits>
#include <mutex>
#include <thread>

using Vertex = std::size_t;
using DistType = int;

class Spinlock {
private:
    std::atomic_flag spinlock = ATOMIC_FLAG_INIT;
public:
    void lock() {
        while (spinlock.test_and_set(std::memory_order_acquire));
    }
    void unlock() {
        spinlock.clear(std::memory_order_release);
    }
};

class QueueElement {
private:
    volatile char padding[128]{};
    std::atomic<DistType> dist;
    std::atomic<int> q_id;
    Spinlock empty_q_id_spinlock;  // lock when changing q_id from empty to something
public:
    size_t index{};
    Vertex vertex;
    explicit QueueElement(Vertex vertex = 0, DistType dist = std::numeric_limits<DistType>::max()) : dist(dist), q_id(-1), vertex(vertex) {}
    QueueElement(const QueueElement & o) : dist(o.dist.load()), q_id(o.q_id.load()), vertex(o.vertex) {}
    void empty_q_id_lock() {
        empty_q_id_spinlock.lock();
    }
    void empty_q_id_unlock() {
        empty_q_id_spinlock.unlock();
    }
    [[noreturn]] QueueElement & operator=(const QueueElement & o) {
        (void)o;
        throw std::logic_error("QueueElement.= shouldn't be used. Probably, BinHeap max size is exceeded.");
    }
    DistType get_dist() const {
        return dist.load();
    }
    void set_dist_relaxed(DistType new_dist) {
        dist.store(new_dist, std::memory_order_relaxed);
    }
    DistType get_dist_relaxed() const {
        return dist.load(std::memory_order_relaxed);
    }
    int get_q_id_relaxed() const {
        return q_id.load(std::memory_order_relaxed);
    }
    void set_q_id_relaxed(int new_q_id) {
        q_id.store(new_q_id, std::memory_order_relaxed);
    }
    bool operator==(const QueueElement & o) const {
        return o.vertex == vertex && o.get_dist() == get_dist();
    }
    bool operator!=(const QueueElement & o) const {
        return !operator==(o);
    }
    bool operator<(const QueueElement & o) const {
        return get_dist() > o.get_dist();
    }
    bool operator>(const QueueElement & o) const {
        return get_dist() < o.get_dist();
    }
    bool operator<=(const QueueElement & o) const {
        return get_dist() >= o.get_dist();
    }
    bool operator>=(const QueueElement & o) const {
        return get_dist() <= o.get_dist();
    }
};

static const DistType empty_element_dist = -1;
static const QueueElement empty_element(0, empty_element_dist);

template<int d = 8>
class my_d_ary_heap {
private:
    size_t size = 0;
    std::vector<QueueElement *> elements;
    Spinlock spinlock;
    std::atomic<QueueElement *> top_element{const_cast<QueueElement *>(&empty_element)};

    void swap(size_t i, size_t j) {
        std::swap(elements[i], elements[j]);
        elements[i]->index = i;
        elements[j]->index = j;
    }
    void sift_up(size_t i) {
        if (size <= 1 || i == 0) {
            top_element.store(elements[0], std::memory_order_relaxed);
            return;
        }
        size_t p = get_parent(i); // everyone except for i == 0 has a parent
        while (*elements[i] > *elements[p]) {
            swap(i, p);
            i = p;
            if (i == 0) break;
            p = get_parent(i);
        }
        top_element.store(elements[0], std::memory_order_relaxed);
    }
    void sift_down(size_t i) {
        if (size == 0) {
            top_element.store(const_cast<QueueElement *>(&empty_element), std::memory_order_relaxed);
            return;
        }
        while (has_at_least_one_child(i)) {
            size_t c = get_biggest_child(i);
            if (*elements[i] > *elements[c]) break;
            swap(i, c);
            i = c;
        }
        top_element.store(elements[0], std::memory_order_relaxed);
    }
    void set(size_t i, QueueElement * element) {
        elements[i] = element;
        elements[i]->index = i;
    }
    static inline size_t get_parent(size_t i) { return (i - 1) / d; }
    bool has_at_least_one_child(size_t i) {
        return i * d + 1 < size;
    }
    size_t get_biggest_child(size_t i) {
        // assume i has at least one child
        int big_i = 1;
        for (int k = 2; (k <= d) && (i * d + k < size); k++) {
            if (*elements[i * d + big_i] < *elements[i * d + k]) {
                big_i = k;
            }
        }
        return i * d + big_i;
    }
public:
    explicit my_d_ary_heap(size_t reserve_size) {
        elements.resize(reserve_size);
    }
    my_d_ary_heap(const my_d_ary_heap & o) = delete;
    my_d_ary_heap(my_d_ary_heap&& o) noexcept :elements(std::move(o.elements)) {};
    my_d_ary_heap& operator=(const my_d_ary_heap & o) = delete;
    my_d_ary_heap& operator=(my_d_ary_heap && o) noexcept {
        elements = std::move(o.elements);
        return *this;
    }
    bool empty() const {
        return size == 0;
    }
    QueueElement * top() const {
        return empty() ? const_cast<QueueElement *>(&empty_element) : elements.front();
    }
    QueueElement * top_relaxed() const {
        return top_element.load(std::memory_order_relaxed);
    }
    void pop() {
        --size;
        elements[0]->index = -1;
        set(0, elements[size]);
        sift_down(0);
    }
    void push(QueueElement * element) {
        size++;
        if (size == elements.size()) {
            throw std::logic_error("my_d_ary_heap reserve size is exceeded");
        }
        set(size - 1, element);
        sift_up(size - 1);
    }
    void decrease_key(QueueElement * element, int new_dist) {
        if (new_dist < element->get_dist()) { // redundant if?
            element->set_dist_relaxed(new_dist);
            size_t i = element->index;
            sift_up(i);
        }
    }
    void lock() {
        spinlock.lock();
    }
    void unlock() {
        spinlock.unlock();
    }
};

#endif //MULTIQUEUE_BINARY_HEAP_H
