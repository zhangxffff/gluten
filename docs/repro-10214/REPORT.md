# Gluten Hash-Shuffle 小 Batch 问题:分析与复现计划

> 关联 issue: [#10214](https://github.com/apache/gluten/issues/10214) ・ 关联 PR: [#10499](https://github.com/apache/gluten/pull/10499), [#10991](https://github.com/apache/gluten/pull/10991)

---

## 1. 背景与结论(分析阶段已完成)

### 1.1 原始问题

Issue #10214 报告:hash-based columnar shuffle 在 partition 数高时(1920 partitions)从 1.6M 输入 batch 放大到 ~148M 输出 batch(~100×),`deserializeTime` 显著占用整 stage 时间,下游 sort/window/join/UDF 都跟着慢。

### 1.2 PR #10499 实际做了什么

把"一个 input stream 对应一个 deserializer"改成"一个 reducer task 共用一个 deserializer,所有 input streams 通过 `Iterator[(BlockId, InputStream)]` 一次性交给 native"。

关键代码位置:

| 角色 | 路径 |
|---|---|
| 入口 reader | `backends-velox/src/main/scala/org/apache/spark/shuffle/ColumnarShuffleReader.scala:99-101` |
| 抽象类 | `backends-velox/src/main/scala/org/apache/gluten/vectorized/ColumnarBatchSerializerInstance.scala:30` |
| 反序列化主体 | `backends-velox/src/main/scala/org/apache/gluten/vectorized/ColumnarBatchSerializer.scala:138-151` |
| JNI 适配 | `gluten-arrow/src/main/scala/org/apache/gluten/vectorized/ShuffleStreamReader.scala:32-42` |
| Native 入口 | `cpp/core/jni/JniWrapper.cc:1235-1248` ・ `cpp/core/jni/JniWrapper.cc:222-233` |
| Native 反序列化 | `cpp/velox/shuffle/VeloxShuffleReader.cc` |

### 1.3 三种 deserializer 的真实行为

在 `cpp/velox/shuffle/VeloxShuffleReader.cc`:

| 类型 | 跨 stream 累积? | 关键代码 |
|---|---|---|
| **`VeloxSortShuffleReaderDeserializer`** (kSortShuffle) | ✅ 真正做了 | `while (cachedRows_ < batchSize_)`,EOF 时 `loadNextStream()` 后继续累积 (L596–622) |
| **`VeloxHashShuffleReaderDeserializer`** (kHashShuffle) | ❌ 完全没做 | 每次只反序列化一个 block payload 立即返回,`numRows` = 写入端 block 的 row 数 (L512–543) |
| **`VeloxRssSortShuffleReaderDeserializer`** (kRssSortShuffle) | ❌ 只在单 stream 内累积 | `while (rowVector->size() < batchSize_ && in_->hasNext())` —— `hasNext()` false 即返回,不跨 stream (L740–768) |

PR 描述本身承认:_"There are no benefits for hash-based shuffle reader and rss-sort shuffle reader."_

### 1.4 后续 PR #10991 的姿态

后续 PR 在 `gluten-substrait/src/main/scala/org/apache/gluten/config/GlutenConfig.scala:32-60` 引入 trait flag:

```scala
case object HashShuffleWriterType extends ShuffleWriterType {
  override val requiresResizingShuffleInput: Boolean = true   // 仍需要 resize
  override val requiresResizingShuffleOutput: Boolean = true
}
case object SortShuffleWriterType extends ShuffleWriterType {
  override val requiresResizingShuffleInput: Boolean = false  // 已被 #10499 在 native 内解决
  override val requiresResizingShuffleOutput: Boolean = false
}
```

这是**明确分工**而非疏忽:sort 内部攒、hash 靠 `VeloxResizeBatchesExec` 兜底。

### 1.5 关键陷阱:默认 conf 下 hash output resize 是关的

```
COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_INPUT   默认 true   (VeloxConfig.scala:314)
COLUMNAR_VELOX_RESIZE_BATCHES_SHUFFLE_OUTPUT  默认 false  (VeloxConfig.scala:323)
```

`AppendBatchResizeForShuffleInputAndOutput.scala:38` 又是两条 conf 都关时直接 return,加上 #10991 的 trait flag —— 意味着:**默认配置下,hash shuffle 的小 batch 直接喂给下游,没有任何兜底**。这是 #10214 报告人实际遭遇的场景。

---

## 2. 待验证的两个假设

复现要回答的核心问题不是"问题存在吗"(代码上已经证实),而是:

| Hypothesis | 期望测得的现象 | 决定后续行动 |
|---|---|---|
| **H1**: 默认 conf 下 hash shuffle 输出 batch 极小,下游被拖慢 | `avgReadBatchNumRows` ≪ `maxBatchSize`(默认 4096),`deserializeTime` 显著 | 证明现象真实,issue #10214 复现成功 |
| **H2**: 即使打开 `resizeBatches.shuffleOutput=true`,仍有可观的剩余开销可被 reader-side merge 消除 | `VeloxResizeBatchesExec` 时间在 stage 中占比 ≥ 5%,或反复"小 batch 发出 → 立即合并"的 JNI 边界开销可见 | 决定是否值得提 PR;只有 H2 也成立,reader-side merge 才有意义 |

如果 H1 成立但 H2 不成立(打开 conf 就够了),那么社区的现状就是"知道不优雅但够用",**提 PR 不会被优先 merge**;只有 H2 也成立,改造才有结构性价值。

---

## 3. 沙盒受限说明(本机为什么没跑)

在 Claude 主机上尝试设置环境时遇到的限制:

- **环境**: Ubuntu 24.04, Linux 6.18.5, Java 21, Maven 3.9, 4 核 / 15GB RAM / 30GB 磁盘
- **Gluten Maven Central**: `https://repo1.maven.org/maven2/org/apache/gluten/` 返回 404 —— Gluten 没有 release 到 Maven Central
- **Spark 下载**: `archive.apache.org` 速度 ~60KB/s,400MB 包 ~2 小时,实测 180s 才到 11MB,放弃
- **源码构建 Gluten**: 需要先构建 Velox(数小时,30GB 磁盘紧张),且需匹配 OS / libstdc++ 版本
- **沙盒禁用 PyPI 下载**(host not in allowlist 或被中断)

因此完整复现需要在外部一台合规机器上跑。下方第 4 节给出最小步骤。

---

## 4. 复现步骤(目标机器)

### 4.1 机器要求

- Linux x86_64(Gluten 1.4 主要支持的 distro: CentOS Stream 8/9, Ubuntu 20.04/22.04)
- ≥ 16 GB RAM,≥ 4 核,≥ 50 GB 磁盘
- Java 8 或 11(Spark 3.5 / Gluten 1.4 主线)
- Apache Spark 3.5.x
- Gluten 预编译 jar(从 Apache nightly 或 release;若 distro 不匹配,需源码构建)

### 4.2 获取 Gluten

**路径 A(推荐)**: 用 Apache Gluten release 的 binary jar,例如:

```bash
# 参考 https://gluten.apache.org/docs/getting-started/build-guide/
# 或下载 nightly: https://github.com/apache/gluten/releases
wget <gluten-velox-bundle.jar>
```

**路径 B**: 从源码构建(2~4 小时)

```bash
git clone https://github.com/apache/gluten.git
cd gluten
./dev/buildbundle-veloxbe.sh  # 会拉取 vcpkg + 构建 Velox + 打包
# 产物在 package/target/gluten-velox-bundle-spark3.5_2.12-*.jar
```

### 4.3 运行脚本

本仓库下 `docs/repro-10214/` 提供:

- `repro_small_batch.py` — PySpark 复现脚本,自动跑三组对照
- `run.sh` — 启动器,带建议 Gluten conf
- `README.md` — 简要操作指南

### 4.4 三组对照实验

| Group | Shuffle 类型 | `resizeBatches.shuffleOutput` | 预期 `avgReadBatchNumRows` |
|---|---|---|---|
| A (问题复现) | hash (default) | false (default) | **远小于 4096(几十~几百)** |
| B (现有兜底) | hash | true | ~4096 |
| C (对照: sort 已解决) | sort | false | ~4096(deserializer 内部已攒,#10499) |

A vs C:**验证 H1**(问题存在,且只在 hash 上);
A vs B:**量化兜底收益**(打开 conf 能恢复多少性能);
B 上 profile:**验证 H2**(是否还有 reader-side merge 的空间)。

### 4.5 关键观测指标

通过 Spark UI(`http://<driver>:4040` → SQL tab → 这个 query 的 detail):

- `avg read batch num rows` (`avgReadBatchNumRows`)— **主指标**
- `time to deserialize` (`deserializeTime`)
- `number of input batches` (`inputBatches`)
- `number of output rows` (`numOutputRows`)
- 算子级:`VeloxResizeBatches` 的 `time` 指标(仅 group B)
- Stage 总时间

`repro_small_batch.py` 会在结束时调用 metric API 把上述数字 dump 到 `results.json`。

### 4.6 实验参数选择(已计算过预期效果)

```
N = 50_000_000      # 总行数
M = 200             # map tasks (即 spark.range 第 4 个参数)
P = 2000            # spark.sql.shuffle.partitions
rows_per_block ≈ N / (M × P) = 50M / (200 × 2000) ≈ 125
```

每个 (mapTask, reducerPartition) 的 block 大约 **125 行**,远小于 `maxBatchSize=4096`。group A 应该看到 `avgReadBatchNumRows ≈ 125`。

如果机器小,可改成 N=10M / M=100 / P=1000(per-block 100 行)。

### 4.7 Profile(可选,用于 H2)

在 group B 上 attach async-profiler:

```bash
./async-profiler/profiler.sh -d 60 -e cpu -f profile-B.html <executor-pid>
```

关注火焰图:

- `gluten::VeloxResizeBatches*` / Velox `RowVector::append` 的占比
- `VeloxHashShuffleReaderDeserializer::next` 内 `BlockPayload::deserialize` 的频率
- JNI 边界 `Java_org_apache_gluten_vectorized_ShuffleReaderJniWrapper_*` 的占用

若上述合计 ≥ 5% stage time → H2 成立,reader-side merge 有结构性收益空间;若 < 1% → H2 不成立,不必继续。

---

## 5. 实验完成后的判断流程

```
        avgReadBatchNumRows(A) << 4096 ?
          │
          ├── 否 → 默认 conf 下问题不显著(环境异常,或参数不够极端)
          │       回到第 4.6 节,提高 P 或降低 M 重新跑
          │
          └── 是 → H1 成立(问题复现 ✓)
                │
                ├── 比较 stage time: A 显著慢于 B?
                │   ├── 否 → 默认 conf 下 hash 的 small batch 影响有限,issue 价值低
                │   └── 是 → 现象明显,继续看 B 是否还有空间
                │
                └── profile group B
                    ├── VeloxResizeBatches + 小 batch JNI 边界 < 1% stage time
                    │   → H2 不成立。社区现状"开 conf 就够",不必提 PR
                    │   → 退化为文档/配置 UX 改进(让默认开 shuffleOutput=true?)
                    │
                    └── ≥ 5% stage time
                        → H2 成立 ✓
                        → 在 #10214 下评论补证据(profile 截图、avg batch 数、占比)
                        → @marin-ma / @FelixYBW,确认 reader-side merge 方向
                        → 拿到正面信号后发 draft PR(只做 simple-schema happy path)
```

---

## 6. PR 路径的工程约束(如果走到那一步)

reader-side merge 的实现思路(在 `VeloxHashShuffleReaderDeserializer::next()` 内):

1. 用 `InMemoryPayload` 接 `BlockPayload::deserialize` 的输出(现成的 `InMemoryPayload::merge` 在 `cpp/core/shuffle/Payload.cc:433-507` 已实现,写入侧 `LocalPartitionWriter.cc:250` 在用)
2. 累积到 `cachedRows_ >= batchSize_` 或遇到不可 merge 边界(complex type / dict 切换 / EOS)再 `toColumnarBatch` 返回
3. 形态参照 `VeloxSortShuffleReaderDeserializer::next()` (L596–622)
4. `HashShuffleWriterType.requiresResizingShuffleOutput` 在 simple-schema 下可置 `false`,复杂类型/dict 仍走 fallback

需要处理的边界:

- **复杂类型**: `InMemoryPayload::mergeable() == false` when `hasComplexType_`。需要 fallback 到现有路径或保留 ResizeBatches
- **Dictionary**: 跨 dict 边界需 flush
- **Buffer 可调整性**: 累加器需显式 `AllocateResizableBuffer`,避免首次 merge 的 `memcpy(sourceSize)` 退化

---

## 7. 文件清单

本 PR 添加的复现包:

```
docs/repro-10214/
├── REPORT.md                  # 本文件:背景 + 复现计划
├── README.md                  # 5 分钟操作指南
├── repro_small_batch.py       # PySpark 复现脚本(三组对照)
└── run.sh                     # 启动器,包含 Gluten conf
```

---

## 8. 待办(留给后续会话或目标机器)

- [ ] 在目标机器跑 `run.sh`,产出 `results.json`
- [ ] 把 group A 的 `avgReadBatchNumRows` / stage time / `deserializeTime` 贴回此文档第 5 节
- [ ] 在 group B 上 profile,判断 H2
- [ ] 据此决定:加 issue 评论 / 文档改进 / draft PR
