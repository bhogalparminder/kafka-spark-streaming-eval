import glob
import os
import pandas as pd
import matplotlib.pyplot as plt

BASE = os.path.expanduser("~/paper_project/results")
OUT = os.path.join(BASE, "exp2", "analysis")

TRIGGER_SEC = 5
WARMUP_SEC = 120

RUNS = {
    "Partitions=3 (baseline)": os.path.join(BASE, "exp1", "rate_8000"),
    "Partitions=6": os.path.join(BASE, "exp2", "kafka_partitions_6"),
    "Partitions=12": os.path.join(BASE, "exp2", "kafka_partitions_12"),
    "Partitions=12 + Spark local[8]": os.path.join(BASE, "exp2", "kafka12_spark_local8"),
}

def read_run(path):
    files = glob.glob(os.path.join(path, "**", "*.csv"), recursive=True)
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_csv(f))
        except Exception:
            pass
    df = pd.concat(dfs, ignore_index=True)

    df = df.sort_values("epoch_sec")
    t0 = df["epoch_sec"].min() + WARMUP_SEC
    df = df[df["epoch_sec"] >= t0]

    df["throughput_eps"] = df["batch_events"] / TRIGGER_SEC
    return df

rows = []

for label, path in RUNS.items():
    df = read_run(path)
    rows.append({
        "configuration": label,
        "batches_used": len(df),
        "throughput_eps_mean": df["throughput_eps"].mean(),
        "lat_p95_ms_avg": df["p95_latency_ms"].mean(),
        "lat_p95_ms_max": df["p95_latency_ms"].max(),
    })

summary = pd.DataFrame(rows)
summary.to_csv(os.path.join(OUT, "exp2_summary.csv"), index=False)
print(summary)

# Plot: p95 latency by configuration
plt.figure()
plt.bar(summary["configuration"], summary["lat_p95_ms_avg"])
plt.ylabel("Average p95 latency (ms)")
plt.title("Experiment 2: Effect of Kafka Partitions and Spark Parallelism")
plt.xticks(rotation=20, ha="right")
plt.grid(axis="y", linestyle="--", linewidth=0.5)

plt.savefig(os.path.join(OUT, "exp2_p95_latency.png"), dpi=200, bbox_inches="tight")
print("Saved exp2_p95_latency.png")
