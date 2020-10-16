#ifndef MULTIQUEUE_BINARY_HEAP_H
#define MULTIQUEUE_BINARY_HEAP_H

#include <atomic>
#include <unordered_map>
#include <vector>
#include <limits>

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

using Vertex = std::size_t;
using DistType = int;

class QueueElement {
private:
    volatile char padding[128]{};
public:
    Vertex vertex;
    std::atomic<DistType> dist;
    std::atomic<int> q_id;
    explicit QueueElement(Vertex vertex = 0, DistType dist = std::numeric_limits<DistType>::max()) : vertex(vertex), dist(dist), q_id(-1) {}
    QueueElement(const QueueElement & o) : vertex(o.vertex), dist(o.dist.load()), q_id(o.q_id.load()) {}
    QueueElement & operator=(const QueueElement & o) {
        if (this != &o) {
            q_id = -1;
            dist.store(o.dist);
            vertex = o.vertex;
        }
        return *this;
    }
    bool operator==(const QueueElement & o) const {
        return o.vertex == vertex && o.dist == dist;
    }
    bool operator!=(const QueueElement & o) const {
        return !operator==(o);
    }
    bool operator<(const QueueElement & o) const {
        return dist > o.dist;
    }
    bool operator>(const QueueElement & o) const {
        return dist < o.dist;
    }
};

static const DistType EMPTY_ELEMENT_DIST = -1;
static const QueueElement EMPTY_ELEMENT(0, EMPTY_ELEMENT_DIST);

class BinaryHeap {
private:
    size_t size = 0;
    std::vector<QueueElement *> elements;
    Spinlock spinlock;
    std::unordered_map<QueueElement *, size_t> element_to_index{};

    void swap(size_t i, size_t j) {
        QueueElement * t = elements[i];
        elements[i] = elements[j];
        elements[j] = t;
        element_to_index[elements[i]] = i;
        element_to_index[elements[j]] = j;
    }
    void sift_up(size_t i) {
        if (size <= 1) return;
        if (i == 0) return;
        size_t p = get_parent(i); // everyone except for i == 0 has a parent
        while (*elements[i] > *elements[p]) {
            swap(i, p);
            i = p;
            if (i == 0) break;
            p = get_parent(i);
        }
    }
    void sift_down(size_t i) {
        if (size == 0) return;
        while (get_left_child(i) < size) {
            size_t l = get_left_child(i);
            size_t r = get_right_child(i);
            size_t j = r < size && *elements[r] > *elements[l] ? r : l;
            if (*elements[i] > *elements[j]) break;
            swap(i, j);
            i = j;
        }
    }
    static inline size_t get_parent(size_t i) { return (i - 1) / 2; }
    static inline size_t get_left_child(size_t i) { return i * 2 + 1; }
    static inline size_t get_right_child(size_t i) { return i * 2 + 2; }
public:
    explicit BinaryHeap(size_t reserve_size = 256) {
        elements.reserve(reserve_size);
        element_to_index.reserve(reserve_size);
        elements.resize(1);
    }
    BinaryHeap(const BinaryHeap & o) : elements(std::vector<QueueElement *>(o.elements.capacity())) {}
    BinaryHeap& operator=(const BinaryHeap & o) {
        std::size_t reserve_size = o.elements.capacity();
        elements.reserve(reserve_size);
        element_to_index.reserve(reserve_size);
        return *this;
    }
    bool empty() const {
        return size == 0;
    }
    QueueElement * top() const {
        return empty() ? (QueueElement *)&EMPTY_ELEMENT : elements.front();
    }
    void pop() {
        element_to_index.erase(element_to_index.find(elements[0]));
        elements[0] = elements[--size];
        element_to_index[elements[0]] = 0;
        sift_down(0);
    }
    void push(QueueElement * element) {
        size++;
        if (size == elements.size()) {
            elements.resize(elements.size() * 4);
        }
        elements[size - 1] = element;
        element_to_index[element] = size - 1;
        sift_up(size - 1);
    }
    void decrease_key(QueueElement * element, int new_dist) {
        if (new_dist < element->dist) { // redundant if?
            element->dist = new_dist;
        }
        size_t i = element_to_index[element];
        sift_up(i);
    }
    void lock() {
        spinlock.lock();
    }
    void unlock() {
        spinlock.unlock();
    }
};

#endif //MULTIQUEUE_BINARY_HEAP_H
