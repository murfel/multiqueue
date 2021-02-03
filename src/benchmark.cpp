#include <benchmark/benchmark.h>
#include <iomanip>

#include "dijkstra.h"
#include "thread_barrier.h"

using Implementation = std::pair<std::function<SsspDijkstraDistsAndStatistics(const AdjList &)>, std::string>;
using BindedImpl = std::pair<std::function<SsspDijkstraDistsAndStatistics()>, std::string>;

class Config {
public:
    Config(const std::vector<std::pair<int, int>> &params, const AdjList &graph, size_t one_queue_reserve_size,
           bool check, bool run_seq) : params(params), graph(graph), one_queue_reserve_size(one_queue_reserve_size),
                                       check(check), run_seq(run_seq || check) {}
    std::vector<std::pair<int, int>> params;
    AdjList graph;
    std::size_t one_queue_reserve_size;
    bool check;
    bool run_seq;
};

static void BM_benchmark(benchmark::State& state, const BindedImpl & impl) {
    for (auto _ : state)
        impl.first();
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

std::chrono::milliseconds measure_time(std::function<void()> func_to_test) {
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

AdjList read_input(std::string filename) {
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

std::vector<Implementation> create_impls(std::vector<std::pair<int, int>> params, bool run_seq,
        size_t one_queue_reserve_size) {
    std::vector<Implementation> impls;
    if (run_seq) {
        auto sequential_dijkstra = [](const AdjList &graph) {
            return calc_sssp_dijkstra_sequential(graph);
        };
        impls.emplace_back(sequential_dijkstra, "Sequential");
    }
    for (const auto & param: params) {
        int num_threads = param.first;
        int size_multiple = param.second;
        const QueueFactory multi_queue_factory = [num_threads, size_multiple, one_queue_reserve_size]()
                { return std::make_unique<MultiQueue<QueueElement>>
                (num_threads, size_multiple, EMPTY_ELEMENT, one_queue_reserve_size); };
        std::string impl_name = std::to_string(num_threads) + " " + std::to_string(size_multiple);
        impls.emplace_back([num_threads, multi_queue_factory] (const AdjList & graph)
                { return calc_sssp_dijkstra(graph, num_threads, multi_queue_factory); }, impl_name);
    }
    return impls;
}

std::vector<BindedImpl> bind_impls(std::vector<Implementation> impls, const AdjList &graph) {
    std::vector<BindedImpl> binded_impls;
    for (const auto & impl : impls) {
        binded_impls.push_back(std::make_pair(std::bind(impl.first, graph), impl.second));
    }
    return binded_impls;
}

bool are_mismatched(const DistVector & correct_answer, const DistVector & to_check) {
    auto mismatch = std::mismatch(correct_answer.begin(), correct_answer.end(), to_check.begin());
    if (mismatch.first != correct_answer.end()) {
        std::cerr << "Mismatch: " << *mismatch.first << " != " << *mismatch.second << std::endl;
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
void check(std::vector<BindedImpl> impls) {
    DistVector correct_answer;
    for (std::size_t i = 0; i < impls.size(); i++) {
        const auto & f = impls[i].first;
        const auto & impl_name = impls[i].second;

        auto p = measure_time<SsspDijkstraDistsAndStatistics>(f);
        auto dists_and_statistics = p.first;
        auto time_ms = p.second;

        std::cerr << impl_name << " elapsed time: " << time_ms.count() << " ms" << std::endl;
        const DistVector &dists = dists_and_statistics.get_dists();

        bool mismatched = false;
        if (i == 0) {
            correct_answer = dists;
        } else {
            mismatched = are_mismatched(correct_answer, dists);
        }

        if (mismatched) {
            std::ofstream output(impl_name + ".out" + std::to_string(i));
            std::chrono::milliseconds elapsed = measure_time([&output, &dists]() { write_answer(output, dists); });
            std::cerr << "Writing elapsed time: " << elapsed.count() << " s" << std::endl;
        }
    }
}

void ops_thread_routine(Multiqueue<QueueElement> & q, thread_barrier & barrier, uint64_t & num_ops, bool monotonic) {
    const std::size_t max_value = (std::size_t)1e8;
    const std::size_t max_elements = (std::size_t)1e8;

    barrier.wait();
if (monotonic) {
    std::default_random_engine generator;
    std::uniform_int_distribution<DistType> distribution(1, 100);
    auto dice = std::bind ( distribution, generator );
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
    std::default_random_engine generator;
    std::uniform_int_distribution<DistType> distribution(0, max_value);
    auto dice = std::bind ( distribution, generator );
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

void throughput_benchmark(std::size_t num_threads, std::size_t size_multiple, bool monotonic) {
    const std::size_t init_size = (std::size_t)1e6;
    const std::size_t max_value = (std::size_t)1e8;
    const std::size_t max_elems = (std::size_t)1e8;

    std::default_random_engine generator;
    std::uniform_int_distribution<int> distribution(0, max_value);
    auto dice = std::bind ( distribution, generator );

    Multiqueue<QueueElement> q(num_threads, size_multiple, EMPTY_ELEMENT, max_elems);
    for (std::size_t i = 0; i < init_size; i++) {
        q.push(QueueElement(1, dice()));
    }
    std::vector<uint64_t> num_ops_counters(num_threads);
    std::vector<std::thread> threads;
    thread_barrier barrier(num_threads);
    for (std::size_t thread_id = 0; thread_id < num_threads; thread_id++) {
        threads.emplace_back(ops_thread_routine, std::ref(q), std::ref(barrier), std::ref(num_ops_counters[thread_id]), monotonic);
        #ifdef __linux__
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(thread_id, &cpuset);
        int rc = pthread_setaffinity_np(threads.back().native_handle(), sizeof(cpu_set_t), &cpuset);
        (void)rc;
        #endif
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
            std::cerr << param.first << " " << param.second << std::endl;
        }
        for (auto & param: config.params) {
            throughput_benchmark(param.first, param.second, true);
        }
        return 0;
    }
    auto impls = create_impls(config.params, config.run_seq, config.one_queue_reserve_size);
    auto binded_impls = bind_impls(impls, config.graph);
    if (config.check) {
        check(binded_impls);
    } else {
        for (const auto & impl : binded_impls) {
            benchmark::RegisterBenchmark(impl.second.c_str(), &BM_benchmark, impl);
        }
        int pseudo_argc = 1;
        benchmark::Initialize(&pseudo_argc, argv);
        benchmark::RunSpecifiedBenchmarks();
    }
}
