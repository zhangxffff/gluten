# 快速复现指南

> 完整背景见 `REPORT.md`。这份只讲怎么跑。

## 前置

- Linux x86_64,Java 8/11
- Spark 3.5.x 已安装(`$SPARK_HOME` 设好,`spark-submit` 在 PATH)
- Gluten Velox bundle jar(从 Apache nightly 下载,或 `./dev/buildbundle-veloxbe.sh` 构建)
- ≥ 16 GB RAM,≥ 4 核

## 一行命令

```bash
export GLUTEN_JAR=/path/to/gluten-velox-bundle-spark3.5_2.12-*.jar
./run.sh
```

跑完后看 `results-A.json`、`results-B.json`、`results-C.json`,以及 `summary.md`。

## 三组对照在做什么

| Group | conf | 期望现象 |
|---|---|---|
| **A** | hash + `shuffleOutput=false`(默认) | `avgReadBatchNumRows ≪ 4096`,stage 慢 |
| **B** | hash + `shuffleOutput=true` | `avgReadBatchNumRows ≈ 4096`,stage 快 |
| **C** | sort | `avgReadBatchNumRows ≈ 4096`,无 resize 算子 |

A 慢于 B 即证明默认 hash 路径有小 batch 性能损失;
A 与 C 对比凸显 sort 已被 #10499 解决,hash 还没。

## 想加 profile

```bash
# 启动 async-profiler 然后跑 B
ASYNC_PROFILER_HOME=/path/to/async-profiler ./run.sh --profile B
```

输出 `profile-B.html`,关注:

- `gluten::VeloxResizeBatches*`
- `VeloxHashShuffleReaderDeserializer::next`
- `BlockPayload::deserialize`
- JNI 边界 `Java_org_apache_gluten_vectorized_ShuffleReaderJniWrapper_*`

合计 ≥ 5% stage CPU → 有 reader-side merge 价值;< 1% → 没必要。

## 参数调整

`repro_small_batch.py` 顶部:

```python
N = 50_000_000      # 总行数
M = 200             # 输入 partitions(map tasks)
P = 2000            # 输出 partitions(reducers)
```

小机器可改 `N=10_000_000, M=100, P=1000`,效果一样(每个 block ~100 行)。
