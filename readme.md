# Multiqueue

## Intro

This is an implementation of a _relaxed_ parallel priority queue, Multiqueue, described in a 2014 [paper](https://arxiv.org/abs/1411.1209) by Rihani, Sanders, Dementiev.

_Relaxed_ means that the `deleteMin` operation of the priority queue is allowed to return something close to a minimum. The relaxation allows to build faster data structures which are still usable in many applications. One of the many applications of the parallel priority queues are parallelized graph algorithms for shortest path or minimal spanning trees, e.g. Dijkstra.

The multiqueue data structure consists of `K * num_threads` (K is usually 4) single-threaded priority queues. To push, push to a random queue; to pop, peek two random queues and pop the lower value.

The goal of this project is to explore, fine-tune, and benchmark multiqueue using the Dijkstra single-source shortest path algorithm.

## Installation
Refer to [Google Benchmark#Installation](https://github.com/google/benchmark#installation) to install Google Benchmark (and Google Test). Perform the actions in the `multiqueue` directory (the repository root).

Run `./download_datasets.sh` to download U.S. map graphs for running Dijkstra on (takes a few minutes). The datasets are released within the [9th DIMACS Challenge](http://users.diag.uniroma1.it/challenge9/).

`cmake CMakeLists.txt && make`

## Usage

Create a parameter file:

`echo "2 4\n4 4" > params.txt`

Each parameter line is a pair of `num_threads` and `K`.

Run a benchmark:

`./mq NY params.txt 256 1 benchmark`

This runs a parallel Dijkstra from the vertex 0 for each parameter line within Google Benchmark. Each Dijkstra instance is run multiple times to calculate statistically plausible average running time. (The number of runs is determined by Google Benchmark).


The 1st argument, `NY` (N=300K,M=700K), is the smallest dataset which is loaded in 600 ms and for which the sequential Dijkstra runs 35 ms on my laptop. `USA` is the biggest dataset (N=23M, M=58M) which is loaded in 15 s and for which the sequential Dijkstra runs 5 s on a super-pupper server with lots of memory and a decent CPU. All available datasets are: `NY BAY COL FLA NW NE CAL LKS E W CTR USA` (uncomment them in `download_datasets.sh`).
If you don't need to run the sequential Dijkstra, use `0` instead of `1` in the 4th argument.

To make just one timed run for each parameter line, use `run`:

`./mq NY params.txt 256 1 run`

To compare the results with the results of the sequential Dijkstra for each parameter line, use `check`:

`./mq NY params.txt 256 1 check`

The 3rd argument is one queue reserve size. It's recommended to avoid memory allocation in parallel programs to avoid synchronization around the new keyword. For provided datasets, maximal queue sizes were less than 256 so this is taken as a default reserve size. 

The general syntax is: `./mq input_filename_no_ext params_filename one_queue_reserve_size run_seq[0,1] [run|check|benchmark]`

## Benchmark results
Benchmarks are run within one NUMA node (18 cores). The performance is degrading when scaling past a NUMA node due to costly cache synchronization between different NUMA nodes. Extra details provided by Google Benchmark:

```
Run on (144 X 3700 MHz CPU s)
CPU Caches:
  L1 Data 32 KiB (x72)
  L1 Instruction 32 KiB (x72)
  L2 Unified 1024 KiB (x72)
  L3 Unified 25344 KiB (x4)
***WARNING*** CPU scaling is enabled, the benchmark real time measurements may be noisy and will incur extra overhead.
***WARNING*** Library was built as DEBUG. Timings may be affected.
```

The sequential version uses `std::priority_queue` and makes `num_verticies` steps.

Ignore the CPU column as it only shows the CPU consumed by the benchmarking thread.

### With std::priority_queue

#### NY (0.84x sequential, slower)

```
$ ./mq NY params.txt 256 1 0
Reading NY: 200 ms
-----------------------------------------------------
Benchmark           Time             CPU   Iterations
-----------------------------------------------------
Sequential       24.6 ms         24.6 ms           26
10 4             36.2 ms         9.95 ms           71
18 4             29.3 ms         9.78 ms           73
36 4             32.1 ms         10.1 ms           70
```

#### USA (2.7x sequential)

```
$ ./mq USA params.txt 256 1 0
Reading USA: 14394 ms
-----------------------------------------------------
Benchmark           Time             CPU   Iterations
-----------------------------------------------------
Sequential       4505 ms         4505 ms            1
10 4             2626 ms          379 ms            1
18 4             1687 ms          369 ms            2
36 4             1962 ms          384 ms            2
```

### With a custom binary heap with decrease_key
#### NY (1.5x sequential)
```
$ ./mq NY params.txt 256 1 benchmark
Reading NY: 188 ms
-----------------------------------------------------
Benchmark           Time             CPU   Iterations
-----------------------------------------------------
Sequential       25.3 ms         25.3 ms           25
10 4             24.5 ms         4.66 ms          150
18 4             16.7 ms         4.78 ms          146
36 4             21.7 ms         5.43 ms          131
```

#### USA (3.3x sequential)
```
$ ./mq USA params.txt 256 1 benchmark
Reading USA: 14338 ms
-----------------------------------------------------
Benchmark           Time             CPU   Iterations
-----------------------------------------------------
Sequential       4437 ms         4437 ms            1
10 4             3164 ms          213 ms            3
18 4             1344 ms          213 ms            3
36 4             1748 ms          262 ms            3
```


## Implementation details

### General

NB: We oftenly refer to _queue_ or _subqueue_ meaning one of the `K * num_threads` single-threaded priority queues. We implement these queues as std::priority_queue or a custom binary heap.

The four parts of the implementation are my_d_ary_heap, Multiqueue, Dijkstra sequential and parallel implementations, and the code for running, checking, and benchmarking.

The parallel Dijkstra algorithm is almost identical to the sequential: while the priority queue is not empty, pop a vertex with the lowest distance (or close to the lowest, in our relaxed case), relax its children and push them to the priority queue. This routine is executed by each thread.

### Binary heap flavors

Currently there are two competing implementations, with `std::priority_queue` (no `decrease_key`) and with a custom binary heap with `decrease_key`. [This commit](https://github.com/murfel/multiqueue/tree/30be79bc9c875095ab354adc4a6097d31f9430e9) contains the `std::priority_queue` implementation. The latest commits contain the implementation with the `decrease_key`.

### Binary heap as `std::priority_queue`

We keep a global vector of atomic distances to each vertex. When relaxing, we compare the new distance to a neighbor vertex with its distance in the global distance array. When pushing, we create a new QueueElement with the vertex number and its new distance at the moment of pushing. When popping an element, we compare the distance with the current global distance and discard the element if it's bigger.

### Custom binary heap with `decrease_key`
Binary heap with `decrease_key` enforces to change the API and the implementation of Multiqueue and Dijkstra. To make use of `decrease_key`, each vertex is now represented as a **unique** `QueueElement` which keeps track of which queue it belongs to. 

### Recommended parameters
The recommended value of `num_threads` is the number of CPU cores (4 for my average laptop), not counting the hyperthreading in, as each thread is expected to be actively busy throughout the calculations.

The recommended value of `K` is 4, which is suggested by the original paper and our benchmarking as well.

### Paddings
We experiment with padding usage to avoid false cache sharing. TODO: Describe paddings used and their performance gains.

### Locks
We use spinlocks based on `std::atomic_flag` in contrast to using `std::mutex` to lock a queue for performing a push or pull as the operations performed with the queues are fast. Moreover, threads will rarely collide and wait for each other at the same queue as there `K` times more queues than there are threads.

We plan to replace the naive spinlock with MCH or CLH cache invalidation optimized locks.

In `Multiqueue.pop`, we use an optimization (described in the paper) of peeking the two top elements without locking the queues and subsequently locking just one queue with the lesser value. If, after locking the queue, the top element has changed, we run the procedure again. In our experiments, this optimization provided a slight performance gain.

### Tests
The test directory contains smoke tests for my_d_ary_heap, Multiqueue, and parallel Dijkstra and longs for extended corner-case and unit testing and coverage.

### Legacy

The Dijkstra header contains some obsolete parts such as the DistsAndStatistics return type. It was earlier used to return statistics calculated during the computations, such as number of pushes, maximal queue sizes, etc. Hovewer, it slowed down the computation and was removed.

The other obsolete part is the `AbstractQueue` class extended by `RegularPriorityQueue`, `BlockingPriorityQueue`, and `MultiQueue` classes which was introduced to compare the performance of Multiqueue on one or a few threads and also check the correctness of the parallel Dijkstra implementation independent of Multiqueue correctness.