#include <benchmark/benchmark.h>
#include <iomanip>

#include <boost/thread/barrier.hpp>
#include <utility>

#include "dijkstra.h"
#include "timer.h"
#include "numa-mq/numa_mq.h"

using Implementation = std::pair<std::function<DistsAndStatistics(const AdjList &, timer &)>, std::string>;
using BindedImpl = std::pair<std::function<DistsAndStatistics(timer &)>, std::string>;

class Config {
public:
    Config(std::vector<std::pair<int, int>> params, AdjList graph, size_t one_queue_reserve_size,
           bool check, bool run_seq)
           : params(std::move(params)), graph(std::move(graph)),
             one_queue_reserve_size(one_queue_reserve_size),
             check(check), run_seq(run_seq || check) {}
    std::vector<std::pair<int, int>> params;
    AdjList graph;
    std::size_t one_queue_reserve_size;
    bool check;
    bool run_seq;
};

static void BM_benchmark(benchmark::State& state, const BindedImpl & impl) {
    for (auto _ : state) {
        (void) _;
        state.PauseTiming();
        timer timer(&state);
        impl.first(timer);
        state.ResumeTiming();
    }
}

std::vector<std::pair<int, int>> read_params(const std::string & params_filename) {
    std::vector<std::pair<int, int>> params;
    std::ifstream params_input(params_filename);
    int num_threads;
    int size_multiple;
    while (params_input >> num_threads >> size_multiple) {
        params.emplace_back(num_threads, size_multiple);
    }
    return params;
}

template<class R>
std::pair<R, std::chrono::milliseconds> measure_time(std::function<R()> func_to_test) {
    auto start = std::chrono::high_resolution_clock::now();
    R r = func_to_test();
    auto end = std::chrono::high_resolution_clock::now();
    return {r, std::chrono::duration_cast<std::chrono::milliseconds>(end - start)};
}

std::chrono::milliseconds measure_time(const std::function<void()>& func_to_test) {
    auto start = std::chrono::high_resolution_clock::now();
    func_to_test();
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
}

AdjList read_edges_into_adj_list(std::istream & istream) {
    const int VERTEX_NUMERATION_OFFSET = -1;
    std::size_t num_verticies, num_edges;
    istream >> num_verticies >> num_edges;
    AdjList adj_list(num_verticies);
    for (std::size_t i = 0; i < num_edges; i++) {
        Vertex from, to;
        DistType weight;
        istream >> from >> to >> weight;
        if (weight <= 0) continue;
        adj_list[from + VERTEX_NUMERATION_OFFSET].emplace_back(to + VERTEX_NUMERATION_OFFSET, weight);
    }
    return adj_list;
}

AdjList read_input(const std::string& filename) {
    std::ifstream input(filename + ".in");
    std::cerr << "Reading " << filename << ": ";
    auto p = measure_time<AdjList>([& input]() { return read_edges_into_adj_list(input); });
    std::chrono::milliseconds time_ms = p.second;
    std::cerr << time_ms.count() << " ms" << std::endl;
    return p.first;
}

Config process_input(int argc, char** argv) {
    if (argc != 6) {
        std::cerr << "Usage: ./dijkstra input_filename_no_ext params_filename one_queue_reserve_size run_seq[0,1] "
                     "check[0,1]"
                  << std::endl;
        exit(1);
    }
    const std::string input_filename(argv[1]);
    const std::string params_filename(argv[2]);
    const std::size_t one_queue_reserve_size = std::stoul(argv[3]);
    const bool run_seq = std::stoi(argv[4]);
    const bool check = std::stoi(argv[5]);

    std::vector<std::pair<int, int>> params = read_params(params_filename);
    AdjList graph;
    if (input_filename != "mops") {
        graph = read_input(input_filename);
    }
    return Config(params, graph, one_queue_reserve_size, check, run_seq);
}

template<class M>
std::vector<Implementation> create_impls(const std::vector<std::pair<int, int>>& params, bool run_seq,
        size_t one_queue_reserve_size) {
    std::vector<Implementation> impls;
    if (run_seq) {
        auto sequential_dijkstra = [](const AdjList &graph, timer& timer) {
            return calc_sssp_dijkstra_sequential(graph, 0, timer);
        };
        impls.emplace_back(sequential_dijkstra, "Sequential");
    }
    for (const auto & param: params) {
        int num_threads = param.first;
        int size_multiple = param.second;
        const QueueFactory<M> multi_queue_factory = [num_threads, size_multiple, one_queue_reserve_size]()
                { return std::make_unique<M>
                (num_threads, size_multiple, EMPTY_ELEMENT, one_queue_reserve_size); };
        std::string impl_name = std::to_string(num_threads) + " " + std::to_string(size_multiple);
        impls.emplace_back([num_threads, multi_queue_factory] (const AdjList & graph, timer& timer)
                { return calc_sssp_dijkstra<M>(graph, num_threads, multi_queue_factory, 0, timer); }, impl_name);
    }
    return impls;
}

std::vector<BindedImpl> bind_impls(const std::vector<Implementation>& impls, const AdjList &graph) {
    std::vector<BindedImpl> binded_impls;
    for (const auto & impl : impls) {
        binded_impls.emplace_back([&impl, &graph](timer& timer) { return impl.first(graph, timer); }, impl.second);
    }
    return binded_impls;
}

bool are_mismatched(const DistVector & correct_answer, const DistVector & to_check) {
    auto mismatch = std::mismatch(correct_answer.begin(), correct_answer.end(), to_check.begin());
    if (mismatch.first != correct_answer.end()) {
        std::cerr << "Mismatch at " << (mismatch.second - to_check.begin()) << ": " <<  *mismatch.first << " != " << *mismatch.second << std::endl;
        return true;
    }
    return false;
}

void write_answer(std::ostream & ostream, const DistVector & dists) {
    for (DistType dist : dists) {
        ostream << dist << '\n';
    }
    ostream << '\n';
}

/* The first implementation should be the reference implementation (sequential). */
void check(std::vector<BindedImpl> impls, int num_iterations) {
    DistVector correct_answer;
    for (std::size_t i = 0; i < impls.size(); i++) {
        for (int iteration = 0; iteration < num_iterations; iteration++) {
            const auto& f = impls[i].first;
            const auto& impl_name = impls[i].second;

            DistVector dists;
            std::thread([&f, &dists](){
                pin_thread_native(0, pthread_self());
                timer timer;
                auto p = measure_time<DistsAndStatistics>([&f, &timer] { return f(timer); });
                auto dists_and_statistics = p.first;

                std::cerr << timer.get_total().count() << std::endl;
                dists = dists_and_statistics.get_dists();
            }).join();

            bool mismatched = false;
            if (i==0) {
                correct_answer = dists;
            }
            else {
                mismatched = are_mismatched(correct_answer, dists);
            }

            if (mismatched) {
                std::ofstream output(impl_name+".out"+std::to_string(i));
                std::chrono::milliseconds elapsed = measure_time([&output, &dists]() { write_answer(output, dists); });
            }
        }
    }
}

template <class MQ>
void ops_thread_routine(MQ & q, boost::barrier & barrier, uint64_t & num_ops, bool monotonic) {
    const auto max_value = (std::size_t)1e8;
    const auto max_elements = (std::size_t)1e8;

    cached_random<uint16_t>::next(q.get_num_queues(), 1'000);

    barrier.wait();
if (monotonic) {
    std::default_random_engine generator{std::random_device()()};
    std::uniform_int_distribution<DistType> distribution(1, 100);
    auto dice = [&distribution, &generator] { return distribution(generator); };
    std::vector<DistType> elements;
    elements.reserve(max_elements);
    for (size_t i = 0; i < max_elements; i++) {
        elements.emplace_back(dice());
    }
    barrier.wait();
    auto start = std::chrono::steady_clock::now();
    int subticks = 1000;
    for (size_t i = 0; i < max_elements; i++) {
        for (int j = 0; j < subticks; i++, j++) {
            auto elem = q.pop();
            q.push({1, elem.get_dist() + elements[i]});
            num_ops += 2;
        }
        auto end = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() > 1000) {
            break;
        }
    }
    barrier.wait();
    return;
}
{
    std::default_random_engine generator{std::random_device()()};
    std::uniform_int_distribution<DistType> distribution(0, max_value);
    auto dice = [&distribution, &generator] { return distribution(generator); };
    std::vector<DistType> elements;
    elements.reserve(max_elements);
    for (size_t i = 0; i < max_elements; i++) {
        elements.emplace_back(dice());
    }
    barrier.wait();
    auto start = std::chrono::steady_clock::now();
    int subticks = 1000;
    for (size_t i = 0; i < max_elements; i++) {
        for (int j = 0; j < subticks; i++, j++) {
            q.push({1, elements[i]});
            q.pop();
            num_ops += 2;
        }
        auto end = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() > 1000) {
            break;
        }
    }
    barrier.wait();
}
}

template <class MQ>
void throughput_benchmark(std::size_t num_threads, std::size_t size_multiple, bool monotonic) {
    const auto init_size = (std::size_t)1e6;
    const auto max_value = (std::size_t)1e8;
    const auto max_elems = (std::size_t)1e8;

    std::default_random_engine generator{std::random_device()()};
    std::uniform_int_distribution<int> distribution(0, max_value);
    auto dice = [&distribution, &generator] { return distribution(generator); };

    pin_thread_native(0, pthread_self());

    MQ q(num_threads, size_multiple, EMPTY_ELEMENT, max_elems);
    cached_random<uint16_t>::next(q.get_num_queues(), 1'000);
    for (std::size_t i = 0; i < init_size; i++) {
        q.push(QueueElement(1, dice()));
    }
    std::vector<uint64_t> num_ops_counters(num_threads);
    std::vector<std::thread> threads;
    boost::barrier barrier(num_threads);
    for (std::size_t thread_id = 0; thread_id < num_threads; thread_id++) {
        threads.emplace_back(ops_thread_routine<MQ>, std::ref(q), std::ref(barrier), std::ref(num_ops_counters[thread_id]), monotonic);
        pin_thread(thread_id, threads.back());
    }
    for (std::thread & thread : threads) {
        thread.join();
    }
//    for (uint64_t num_ops: num_ops_counters) {
//        std::cerr << num_ops / 1'000'000.0 << std::endl;
//    }
    const double mops = std::accumulate(num_ops_counters.begin(), num_ops_counters.end(), 0ULL) / 1'000'000.0;
    std::cerr.precision(2);
    std::cerr << mops << std::endl;
}

int main(int argc, char** argv) {
    Config config = process_input(argc, argv);
    if (config.graph.empty()) {
        for (auto & param: config.params) {
            std::thread([&param]{
                throughput_benchmark<numa_mq<QueueElement>>(param.first, param.second, false);
            }).join();
        }
        return 0;
    }
    auto impls = create_impls<numa_mq<QueueElement>>(config.params, config.run_seq, config.one_queue_reserve_size);
    auto binded_impls = bind_impls(impls, config.graph);
    if (config.check) {
        check(binded_impls, 1);
    } else {
        for (const auto & impl : binded_impls) {
            benchmark::RegisterBenchmark(impl.second.c_str(), &BM_benchmark, impl)->Unit(benchmark::kMillisecond)
                    ->MeasureProcessCPUTime()->Iterations(3);
        }
        int pseudo_argc = 1;
        benchmark::Initialize(&pseudo_argc, argv);
        benchmark::RunSpecifiedBenchmarks();
    }
}
