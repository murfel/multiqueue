#include "gtest/gtest.h"
#include "../src/dijkstra.h"

TEST(Dijkstra, Minimized) {

    std::size_t num_vertexes = 3;
    AdjList graph(num_vertexes, std::vector<Edge>());
    graph[0] = {{1, 1}};
    graph[1] = {{2, 2}};

    QueueFactory q = [](){ return std::make_unique<MultiQueue>(1, 2, 1); };
    DistsAndStatistics x = calc_dijkstra(graph, 1, q);

    DistVector expected = {0, 1, 3};
    DistVector dists = x.get_dists();
    for (std::size_t i = 0; i < num_vertexes; i++) {
        ASSERT_EQ(expected[i], dists[i]);
    }
}

TEST(Dijkstra, Simple) {
    std::size_t num_vertexes = 10;
    AdjList graph(num_vertexes, std::vector<Edge>());
    graph[0] = {{1, 2}, {3, 3}, {4, 5}, {5, 6}};
    graph[1] = {{2, 2}};
    graph[2] = {{6, 2}, {7, 4}, {8, 5}};

    QueueFactory q = [](){ return std::make_unique<MultiQueue>(1, 2, 1); };
    DistsAndStatistics x = calc_dijkstra(graph, 1, q);

    DistVector expected = {0, 2, 4, 3, 5, 6, 6, 8, 9, INT_MAX};
    DistVector dists = x.get_dists();
    for (std::size_t i = 0; i < num_vertexes; i++) {
        ASSERT_EQ(expected[i], dists[i]);
    }
}