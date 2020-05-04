cmake_minimum_required(VERSION 3.16)
project(multiqueue)

set(CMAKE_CXX_STANDARD 17)

add_executable(dijkstra dijkstra.cpp multiqueue.h)
add_executable(multiqueue_tests.cpp multiqueue_tests.cpp multiqueue.h)