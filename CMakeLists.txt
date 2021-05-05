cmake_minimum_required(VERSION 3.7)
project(multiqueue)

#option(USE_CLANG "build application with clang" ON) # OFF is the default
#

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -O0 -Wall -Wextra -pedantic -pthread -DPADDING=128 -DALIGNMENT=128 -DDISTPADDING=not_padded -DQUEUEPADDING=padded -march=native -lnuma -latomic")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -g")

set(BOOST_ROOT "~/boost/")
find_package(Boost REQUIRED COMPONENTS thread)

add_executable(mq src/benchmark.cpp src/dijkstra.h src/multiqueue.h src/cached_random.h src/timer.h src/utils.h src/numa-mq/numa_mq.h spinlock.h)
target_link_libraries(mq PRIVATE benchmark Boost::thread)
target_link_directories(mq PRIVATE ~/benchmark/build/src)
target_include_directories(mq PRIVATE ~/benchmark/include ~/numa-mq)

add_executable(mq_tests src/multiqueue_tests.cpp src/dijkstra.h src/multiqueue.h src/cached_random.h src/timer.h src/utils.h src/numa-mq/numa_mq.h spinlock.h)
target_link_libraries(mq_tests PRIVATE benchmark Boost::thread)
target_link_directories(mq_tests PRIVATE ~/benchmark/build/src)
target_include_directories(mq_tests PRIVATE ~/benchmark/include ~/numa-mq)


set(TEST_SOURCES
        test/main.cpp
        )

add_executable(all_test ${TEST_SOURCES})
target_link_libraries(all_test gtest gtest_main)
add_test(NAME all_test COMMAND all_test)
