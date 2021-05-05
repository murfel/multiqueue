#ifndef MULTIQUEUE_CACHED_RANDOM_H
#define MULTIQUEUE_CACHED_RANDOM_H

#include <vector>
#include <random>

template<class T>
class cached_random {
public:
    T static next(T upto_excluding = 0, std::size_t cache_size = 0) {
        thread_local std::vector<T> values;
        thread_local std::size_t index = 0;
        if (values.empty() && (upto_excluding==0 || cache_size==0)) {
                std::cerr << "The first call to cached_random.next should be with arguments" << std::endl;
                exit(1);
        }
        if (upto_excluding != 0 && cache_size != 0) {
            values.clear();
            std::default_random_engine generator{std::random_device()()};
            std::uniform_int_distribution<T> distribution(0, upto_excluding - 1);
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

#endif //MULTIQUEUE_CACHED_RANDOM_H
