cmake_minimum_required(VERSION 3.7)
project(multiqueue)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -O3 -Wall -Wextra -pedantic -pthread -DPADDING=128 -DALIGNMENT=128 -DDISTPADDING=not_padded -DQUEUEPADDING=padded -march=native")
add_subdirectory(benchmark)

include_directories("/nfs/scistore08/alistgrp/vaksenov/murfel/multiqueue/boost_1_75_0")

add_executable(mq src/benchmark.cpp src/dijkstra.h src/multiqueue.h src/thread_barrier.h)
target_link_libraries(mq LINK_PUBLIC benchmark::benchmark)

set(TEST_SOURCES
        test/main.cpp
        )

add_executable(all_test ${TEST_SOURCES})
target_link_libraries(all_test gtest gtest_main)
add_test(NAME all_test COMMAND all_test)
