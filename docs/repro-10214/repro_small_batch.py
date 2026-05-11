"""
Gluten hash-shuffle small-batch reproducer.

Runs a single benchmark group. Group is passed via env var GROUP=A|B|C.

Group definitions:
  A: hash shuffle, resizeBatches.shuffleOutput=false (default) -- reproduces #10214
  B: hash shuffle, resizeBatches.shuffleOutput=true            -- existing fallback
  C: sort shuffle                                              -- already fixed by #10499

Outputs metrics to results-${GROUP}.json so run.sh can aggregate them.
"""

import json
import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as fsum

# Parameters; override via env vars if the machine is smaller.
N = int(os.environ.get("REPRO_N", "50000000"))
M = int(os.environ.get("REPRO_M", "200"))
P = int(os.environ.get("REPRO_P", "2000"))
GROUP = os.environ.get("GROUP", "A").upper()
OUT = os.environ.get("OUT_FILE", f"results-{GROUP}.json")

# Conf shared by all groups.
common_conf = {
    "spark.master": os.environ.get("SPARK_MASTER", "local[4]"),
    "spark.driver.memory": os.environ.get("SPARK_DRIVER_MEM", "8g"),
    "spark.sql.shuffle.partitions": str(P),
    # Gluten plugin
    "spark.plugins": "org.apache.gluten.GlutenPlugin",
    "spark.memory.offHeap.enabled": "true",
    "spark.memory.offHeap.size": "4g",
    "spark.shuffle.manager": "org.apache.spark.shuffle.sort.ColumnarShuffleManager",
}

# Per-group conf.
group_conf = {
    "A": {
        "spark.gluten.sql.columnar.backend.velox.resizeBatches.shuffleOutput": "false",
        "spark.gluten.sql.columnar.backend.velox.resizeBatches.shuffleInput": "true",
    },
    "B": {
        "spark.gluten.sql.columnar.backend.velox.resizeBatches.shuffleOutput": "true",
        "spark.gluten.sql.columnar.backend.velox.resizeBatches.shuffleInput": "true",
    },
    "C": {
        # Force sort-based columnar shuffle writer to demonstrate #10499 path.
        "spark.gluten.sql.columnar.shuffle.writer": "sort",
        "spark.gluten.sql.columnar.backend.velox.resizeBatches.shuffleOutput": "false",
        "spark.gluten.sql.columnar.backend.velox.resizeBatches.shuffleInput": "true",
    },
}

if GROUP not in group_conf:
    print(f"Unknown GROUP={GROUP}; use A|B|C", file=sys.stderr)
    sys.exit(2)

builder = SparkSession.builder.appName(f"repro-10214-{GROUP}")
for k, v in {**common_conf, **group_conf[GROUP]}.items():
    builder = builder.config(k, v)

spark = builder.getOrCreate()
sc = spark.sparkContext

# Build the query.
# spark.range(start, end, step, numPartitions) controls map task count.
df = (
    spark.range(0, N, 1, M)
    .selectExpr("id", "id % 10000 AS k", "id * 7 AS v")
    .repartition(P, col("k"))            # hash shuffle on k, P reducers
    .groupBy("k")
    .agg(fsum("v").alias("s"))
)

# Force execution.
t0 = time.time()
row_count = df.count()
elapsed = time.time() - t0

# Pull metrics out of the Spark UI status store.
status_store = sc._jsc.sc().statusStore()
exec_iter = status_store.executionsList().iterator()
exec_metrics = {}
while exec_iter.hasNext():
    e = exec_iter.next()
    # most recent execution: take the last one
    em = {}
    metric_iter = e.metrics().iterator() if hasattr(e, "metrics") else iter([])
    # Spark exposes per-accumulator metric values in status store under metricValues
    # Fall back: just keep description
    em["description"] = e.description() if hasattr(e, "description") else ""
    em["duration"] = e.duration() if hasattr(e, "duration") else -1
    exec_metrics[e.executionId()] = em

# Pull SQL metrics from the last execution.
sql_metrics = {}
try:
    status = sc._jsc.sc().statusStore()
    apps = status.executionsList()
    last = apps.get(apps.size() - 1)
    last_id = last.executionId()
    # accumulators are exposed via SQLAppStatusStore.metricValues
    mv = status.executionMetrics(last_id)
    it = mv.entrySet().iterator()
    while it.hasNext():
        e = it.next()
        sql_metrics[str(e.getKey())] = str(e.getValue())
except Exception as exc:  # noqa: BLE001
    sql_metrics["_error"] = repr(exc)

# Also dump the executed plan as text so we can see whether
# VeloxResizeBatches got inserted.
plan_text = df._jdf.queryExecution().executedPlan().toString()

result = {
    "group": GROUP,
    "params": {"N": N, "M": M, "P": P},
    "expected_rows_per_block": round(N / (M * P), 2),
    "row_count": row_count,
    "elapsed_sec": round(elapsed, 3),
    "sql_metrics": sql_metrics,
    "executed_plan": plan_text,
    "conf": {**common_conf, **group_conf[GROUP]},
}

with open(OUT, "w") as f:
    json.dump(result, f, indent=2)

print(f"[group {GROUP}] rows={row_count} elapsed={elapsed:.2f}s -> {OUT}")
spark.stop()
