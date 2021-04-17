#include <thread>
#include <vector>

#include <benchmark/benchmark.h>
#include <boost/thread/barrier.hpp>

void work() {
    volatile int sum = 0;
    for (int i = 0; i < 100'000'000; i++) {
        sum += i;
    }
}

static void thread_routine(boost::barrier& barrier, benchmark::State& state, int thread_id) {
    // do setup here, if needed
    barrier.wait();  // wait until each thread is created
    if (thread_id == 0) {
        state.ResumeTiming();
    }
    barrier.wait();  // wait until the timer is started before doing the work

    // do some work
    work();

    barrier.wait();  // wait until each thread completes the work
    if (thread_id == 0) {
        state.PauseTiming();
    }
    barrier.wait();  // wait until the timer is stopped before destructing the thread
    // do teardown here, if needed
}

void f(benchmark::State& state) {
    const int num_threads = 1000;
    boost::barrier barrier(num_threads);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back(thread_routine, std::ref(barrier), std::ref(state), i);
    }
    for (std::thread& thread : threads) {
        thread.join();
    }
}

static void BM_AlreadyMultiThreaded(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        f(state);
        state.ResumeTiming();
    }
}

BENCHMARK(BM_AlreadyMultiThreaded)->Iterations(10)->Unit(benchmark::kMillisecond)->MeasureProcessCPUTime(); // NOLINT(cert-err58-cpp)
BENCHMARK_MAIN();