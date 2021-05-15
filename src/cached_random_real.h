#ifndef MULTIQUEUE_CACHED_RANDOM_REAL_H
#define MULTIQUEUE_CACHED_RANDOM_REAL_H

#include <vector>
#include <random>

template<class T>
class cached_random_real {
public:
    T static next(T upto_excluding = 0, std::size_t cache_size = 0) {
        thread_local std::vector<T> values;
        thread_local std::size_t index = 0;
        if (values.empty()) {
            if (upto_excluding == 0 || cache_size == 0) {
                std::cerr << "The first call to cached_random.next should be with arguments" << std::endl;
                exit(1);
            }
            std::default_random_engine generator{std::random_device()()};
            std::uniform_real_distribution<T> distribution(0,
                    std::nextafter(upto_excluding, std::numeric_limits<double>::max()));
            auto dice = [&distribution, &generator] { return distribution(generator); };
            for (std::size_t i = 0; i < cache_size; i++) {
                values.push_back(dice());
            }
        }
        // Consider caching enough values, and commenting this out.
        if (index == values.size()) index = 0;
        return values[index++];
    }
};

#endif //MULTIQUEUE_CACHED_RANDOM_REAL_H
