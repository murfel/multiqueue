#include <benchmark/benchmark.h>

#include "dijkstra.h"

using Implementation = std::pair<std::function<SsspDijkstraDistsAndStatistics(const AdjList &, DummyState)>, std::string>;
using BindedImpl = std::pair<std::function<SsspDijkstraDistsAndStatistics(DummyState)>, std::string>;

class Config {
public:
    enum RunType { run, check, benchmark };
    Config(const std::vector<std::pair<int, int>> &params, const AdjList &graph, size_t one_queue_reserve_size,
           RunType run_type, bool run_seq) : params(params), graph(graph), one_queue_reserve_size(one_queue_reserve_size),
                                       run_type(run_type), run_seq(run_seq || run_type == check) {}
    std::vector<std::pair<int, int>> params;
    AdjList graph;
    std::size_t one_queue_reserve_size;
    RunType run_type;
    bool run_seq;
};

static void BM_benchmark(benchmark::State& state, const BindedImpl & impl) {
    for (auto _ : state)
        impl.first(DummyState(&state));
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

void print_usage_error_and_exit() {
    std::cerr << "Usage: ./mq input_filename_no_ext params_filename one_queue_reserve_size run_seq[0,1] "
                 "[run|check|benchmark]"
              << std::endl;
    exit(1);
}

Config process_input(int argc, char** argv) {
    if (argc != 6) {
        print_usage_error_and_exit();
    }
    const std::string input_filename(argv[1]);
    const std::string params_filename(argv[2]);
    const std::size_t one_queue_reserve_size = std::stoul(argv[3]);
    const bool run_seq = std::stoi(argv[4]);
    Config::RunType run_type = Config::run;
    if (strcmp("run", argv[5]) == 0) {
        run_type = Config::run;
    } else if (strcmp("check", argv[5]) == 0) {
        run_type = Config::check;
    } else if (strcmp("benchmark", argv[5]) == 0) {
        run_type = Config::benchmark;
    } else {
        print_usage_error_and_exit();
    }

    std::vector<std::pair<int, int>> params = read_params(params_filename);
    AdjList graph = read_input(input_filename);
    return Config(params, graph, one_queue_reserve_size, run_type, run_seq);
}

std::vector<Implementation> create_impls(std::vector<std::pair<int, int>> params, bool run_seq,
        size_t one_queue_reserve_size) {
    std::vector<Implementation> impls;
    if (run_seq) {
        auto sequential_dijkstra = [](const AdjList &graph, DummyState state) {
            return calc_sssp_dijkstra_sequential(graph);
        };
        impls.emplace_back(sequential_dijkstra, "Sequential");
    }
    for (const auto & param: params) {
        int num_threads = param.first;
        int size_multiple = param.second;
        std::size_t num_bin_heaps = num_threads * size_multiple;
        const QueueFactory multi_queue_factory = [num_threads, size_multiple, one_queue_reserve_size]()
                { return std::make_unique<MultiQueue>(num_threads, size_multiple, one_queue_reserve_size); };
        std::string impl_name = std::to_string(num_threads) + " " + std::to_string(size_multiple);
        impls.emplace_back([num_threads, multi_queue_factory, num_bin_heaps] (const AdjList & graph, DummyState state)
                { return calc_sssp_dijkstra(graph, num_threads, multi_queue_factory, num_bin_heaps, state); }, impl_name);
    }
    return impls;
}

std::vector<BindedImpl> bind_impls(std::vector<Implementation> impls, const AdjList &graph) {
    std::vector<BindedImpl> binded_impls;
    for (const auto & impl : impls) {
        binded_impls.push_back(std::make_pair(std::bind(impl.first, graph, std::placeholders::_1), impl.second));
    }
    return binded_impls;
}

bool are_mismatched(const DistVector & correct_answer, const DistVector & to_check) {
    auto mismatch = std::mismatch(correct_answer.begin(), correct_answer.end(), to_check.begin());
    if (mismatch.first != correct_answer.end()) {
        std::cerr << "Mismatch: " << *mismatch.second << " != " << *mismatch.first << " at i = "
                << std::distance(correct_answer.begin(), mismatch.first) << std::endl;
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

void run(std::vector<BindedImpl> impls) {
    for (std::size_t i = 0; i < impls.size(); i++) {
        const auto & f = impls[i].first;
        const auto & impl_name = impls[i].second;

        auto p = measure_time<SsspDijkstraDistsAndStatistics>(std::bind(f, DummyState()));
        auto time_ms = p.second;

        std::cerr << impl_name << " " << time_ms.count() << " ms" << std::endl;
    }
}

/* The first implementation should be the reference implementation (sequential). */
void run_and_check(std::vector<BindedImpl> impls) {
    DistVector correct_answer;
    for (std::size_t i = 0; i < impls.size(); i++) {
        const auto & f = impls[i].first;
        const auto & impl_name = impls[i].second;

        auto p = measure_time<SsspDijkstraDistsAndStatistics>(std::bind(f, DummyState()));
        auto dists_and_statistics = p.first;
        auto time_ms = p.second;

        std::cerr << impl_name << " " << time_ms.count() << " ms" << std::endl;
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
            std::cerr << "Writing: " << elapsed.count() << " s" << std::endl;
        }
    }
}

int main(int argc, char** argv) {
    Config config = process_input(argc, argv);
    auto impls = create_impls(config.params, config.run_seq, config.one_queue_reserve_size);
    auto binded_impls = bind_impls(impls, config.graph);
    if (config.run_type == Config::run) {
        run(binded_impls);
    } else if (config.run_type == Config::check) {
        run_and_check(binded_impls);
    } else {
        for (const auto & impl : binded_impls) {
            benchmark::RegisterBenchmark(impl.second.c_str(), &BM_benchmark, impl)->Unit(benchmark::kMillisecond)
                    ->MeasureProcessCPUTime()->UseRealTime();
        }
        int pseudo_argc = 1;
        benchmark::Initialize(&pseudo_argc, argv);
        benchmark::RunSpecifiedBenchmarks();
    }
}
