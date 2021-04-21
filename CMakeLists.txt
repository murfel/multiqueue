cmake_minimum_required(VERSION 3.7)
project(multiqueue)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -O3 -Wall -Wextra -pedantic -pthread -DPADDING=128 -DALIGNMENT=128 -DDISTPADDING=not_padded -DQUEUEPADDING=padded -march=native")

set(BOOST_ROOT "~/boost/")
find_package(Boost REQUIRED COMPONENTS thread)

add_executable(mq src/benchmark.cpp src/dijkstra.h src/multiqueue.h src/cached_random.h src/timer.h src/utils.h)
target_link_libraries(mq PRIVATE benchmark Boost::thread)
target_link_directories(mq PRIVATE ~/benchmark/build/src)
target_include_directories(mq PRIVATE ~/benchmark/include)

set(TEST_SOURCES
        test/main.cpp
        )

add_executable(all_test ${TEST_SOURCES})
target_link_libraries(all_test gtest gtest_main)
add_test(NAME all_test COMMAND all_test)
