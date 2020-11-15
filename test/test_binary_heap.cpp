#include "gtest/gtest.h"
#include "../src/binary_heap.h"

TEST(BinaryHeap, Simple) {
    std::vector<std::size_t> dists = {3, 4, 2, 8, 7};
    std::vector<QueueElement> vertexes(dists.size());
    for (std::size_t i = 0; i < dists.size(); i++) {
        vertexes[i].vertex = i;
        vertexes[i].set_dist_relaxed(dists[i]);
    }
    BinaryHeap heap = BinaryHeap();
    ASSERT_TRUE(heap.empty());
    for (std::size_t i = 0; i < dists.size(); i++) {
        heap.push(&vertexes[i]);
        ASSERT_FALSE(heap.empty());
    }
    ASSERT_EQ(2, heap.top()->get_dist());
    heap.decrease_key(&vertexes[3], 1);
    ASSERT_EQ(1, heap.top()->get_dist());

    std::vector<std::size_t> pop_dists = {1, 2, 3, 4, 7};
    for (std::size_t i = 0; i < pop_dists.size(); i++) {
        ASSERT_EQ(pop_dists[i], heap.top()->get_dist());
        ASSERT_FALSE(heap.empty());
        heap.pop();
    }
    ASSERT_TRUE(heap.empty());
}