#ifndef MULTIQUEUE_TIMER_H
#define MULTIQUEUE_TIMER_H

#include <benchmark/benchmark.h>

class timer {
private:
    benchmark::State * state;
    std::chrono::time_point<std::chrono::high_resolution_clock> start{};
    bool running = false;
    std::chrono::milliseconds total{0};
public:
    explicit timer(benchmark::State * state = nullptr) :state(state) {}
    void pause_timing() {
        if (state != nullptr) {
            state->PauseTiming();
        } else {
            if (!running) {
                return;
            }
            auto end = std::chrono::high_resolution_clock::now();
            total += std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            running = false;
        }
    }
    void resume_timing() {
        if (state != nullptr) {
            state->ResumeTiming();
        } else {
            if (running) {
                return;
            }
            start = std::chrono::high_resolution_clock::now();
            running = true;
        }
    }
    std::chrono::milliseconds get_total() {
        return total;
    }
};

#endif //MULTIQUEUE_TIMER_H
