cmake_minimum_required(VERSION 3.7)
project(multiqueue)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -O3 -Wall -Wextra -pedantic -pthread")

add_executable(dijkstra dijkstra.cpp multiqueue.h)
add_executable(multiqueue_tests.cpp multiqueue_tests.cpp multiqueue.h)