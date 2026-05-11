#!/usr/bin/env bash
# Launcher for the three-group small-batch reproducer.
# See REPORT.md for full context.

set -euo pipefail

if [[ -z "${GLUTEN_JAR:-}" ]]; then
  echo "ERROR: set GLUTEN_JAR to your gluten-velox-bundle jar path." >&2
  echo "  e.g. export GLUTEN_JAR=/path/to/gluten-velox-bundle-spark3.5_2.12-*.jar" >&2
  exit 2
fi
if [[ ! -f "${GLUTEN_JAR}" ]]; then
  echo "ERROR: GLUTEN_JAR not found: ${GLUTEN_JAR}" >&2
  exit 2
fi

: "${SPARK_HOME:=$(dirname "$(command -v spark-submit)")/..}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

run_group() {
  local g="$1"
  echo "===== Group $g ====="
  GROUP="$g" OUT_FILE="${SCRIPT_DIR}/results-${g}.json" \
    "${SPARK_HOME}/bin/spark-submit" \
      --master "${SPARK_MASTER:-local[4]}" \
      --jars "${GLUTEN_JAR}" \
      --conf "spark.driver.extraClassPath=${GLUTEN_JAR}" \
      --conf "spark.executor.extraClassPath=${GLUTEN_JAR}" \
      --conf "spark.driver.memory=${SPARK_DRIVER_MEM:-8g}" \
      "${SCRIPT_DIR}/repro_small_batch.py"
}

if [[ "${1:-}" == "--profile" ]]; then
  # Single-group run with async-profiler.
  group="${2:-B}"
  : "${ASYNC_PROFILER_HOME:?set ASYNC_PROFILER_HOME to your async-profiler dir}"
  echo "Profile mode: running group $group only, attach profiler manually."
  echo "Tip: in another shell: ${ASYNC_PROFILER_HOME}/profiler.sh -d 60 -e cpu -f profile-${group}.html \$(pgrep -f repro_small_batch.py | head -1)"
  run_group "$group"
  exit 0
fi

# Standard three-group run.
for g in A B C; do
  run_group "$g"
done

echo
echo "===== Summary ====="
python3 - "${SCRIPT_DIR}" <<'PY'
import json, os, sys, glob
d = sys.argv[1]
rows = []
for g in ("A", "B", "C"):
    p = os.path.join(d, f"results-{g}.json")
    if not os.path.exists(p):
        continue
    j = json.load(open(p))
    m = j.get("sql_metrics", {})
    rows.append({
        "group": g,
        "elapsed_sec": j.get("elapsed_sec"),
        "expected_rows_per_block": j.get("expected_rows_per_block"),
        "avg_read_batch_num_rows": next((v for k, v in m.items() if "avg read batch num rows" in k.lower() or "avgreadbatchnumrows" in k.lower()), "n/a"),
        "deserialize_time": next((v for k, v in m.items() if "deserialize" in k.lower()), "n/a"),
        "input_batches": next((v for k, v in m.items() if "input batches" in k.lower()), "n/a"),
    })
fmt = "{:<6} {:>14} {:>14} {:>26} {:>20} {:>16}"
print(fmt.format("Group", "Elapsed(s)", "RowsPerBlock", "AvgReadBatchNumRows", "DeserializeTime", "InputBatches"))
for r in rows:
    print(fmt.format(r["group"], r["elapsed_sec"], r["expected_rows_per_block"], r["avg_read_batch_num_rows"], r["deserialize_time"], r["input_batches"]))

with open(os.path.join(d, "summary.md"), "w") as f:
    f.write("# Reproduction Summary\n\n")
    f.write("| Group | Elapsed (s) | Expected rows/block | avgReadBatchNumRows | DeserializeTime | InputBatches |\n")
    f.write("|-------|-------------|---------------------|---------------------|-----------------|--------------|\n")
    for r in rows:
        f.write(f"| {r['group']} | {r['elapsed_sec']} | {r['expected_rows_per_block']} | {r['avg_read_batch_num_rows']} | {r['deserialize_time']} | {r['input_batches']} |\n")
print("\nSummary written to summary.md")
PY
