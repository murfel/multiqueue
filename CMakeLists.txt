cmake_minimum_required(VERSION 3.7)
project(multiqueue)

set(CMAKE_CXX_STANDARD 14)

add_executable(dijkstra dijkstra.cpp multiqueue.h)
add_executable(multiqueue_tests.cpp multiqueue_tests.cpp multiqueue.h)