cmake_minimum_required(VERSION 3.7)
project(multiqueue)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall -Wextra -Werror -pedantic -pthread -lnuma -DPADDING=128 -DALIGNMENT=128 -DDIST_PADDING=not_padded -DQUEUE_PADDING=padded")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -g")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lrt")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DXEON -DR730 -DCOMPACT -DUSE_CLH_LOCKS -D_GNU_SOURCE -DADD_PADDING")

set(BOOST_ROOT "~/boost/")
find_package(Boost REQUIRED COMPONENTS thread)

add_executable(mq src/benchmark.cpp src/dijkstra.h src/multiqueue.h src/utils.h)
target_link_libraries(mq PRIVATE benchmark Boost::thread)
target_link_directories(mq PRIVATE ~/benchmark/build/src)
target_include_directories(mq PRIVATE ~/benchmark/include)

add_executable(benchmark_example benchmark_example.cpp)
target_link_libraries(benchmark_example PRIVATE benchmark Boost::thread)
target_link_directories(benchmark_example PRIVATE ~/benchmark/build/src)
target_include_directories(benchmark_example PRIVATE ~/benchmark/include)

# TODO: test_multiqueue and test_dijkstra cause multiple definition of random_fnv1a (from multiqueue.h)
set(TEST_SOURCES
        test/main.cpp
        test/test_binary_heap.cpp
        test/test_multiqueue.cpp
        test/test_dijkstra.cpp
        )

add_executable(all_test ${TEST_SOURCES})
target_link_libraries(all_test gtest gtest_main)
add_test(NAME all_test COMMAND all_test)
