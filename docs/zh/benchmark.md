# 性能

数字来自这台 4 vCPU 的虚拟机，只说明相对变化，不代表裸机。复现：

```bash
cmake -S . -B build-rel -DCMAKE_BUILD_TYPE=Release -DSHM_PUBSUB_BUILD_BENCHMARKS=ON
cmake --build build-rel -j --target throughput_benchmark latency_benchmark control_path_benchmark
./build-rel/throughput_benchmark 1 200000 64
./build-rel/latency_benchmark 2000 64 futex
```

`throughput_benchmark` 接受位置参数 `[订阅者] [消息数] [字节]`，以及 `--publishers`、`--batch`、`--no-timestamps`、`--idle`。

0.4 相对 0.3：忙等读者下的吞吐大约翻倍（1 个订阅者约 2.32M 到 4.44M 消息/秒，2 个订阅者约 1.38M 到 3.36M，中位数，12 次交错运行）。延迟中位数仍在约 4 微秒。端点构造从随池子变大变成大约 2.7 微秒。批量和关掉时间戳在这台机器上落在噪声里。两个发布者同时对满队列做 drop-oldest 时，总吞吐仍由队列位置上的竞争决定，没有提高。

`scripts/compare-bench.sh` 跑本库的延迟基准，并在设置了 `ICEORYX2_PUBSUB` 或 `ECAL_LATENCY` 时附上那两个程序的输出。这台 4 vCPU 虚拟机上，2000 次 64 字节 futex 投递的 p50 是 3.937 微秒，p99 是 5.245 微秒（`docs/comparison-vm.csv`）。iceoryx2 和 eCAL 当时没有安装。
