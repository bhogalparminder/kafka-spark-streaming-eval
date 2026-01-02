import glob
import pandas as pd
import os

BASE = os.path.expanduser("~/paper_project/results/exp3/recovery_test")

# Load all Spark CSV metric files
files = glob.glob(os.path.join(BASE, "**", "*.csv"), recursive=True)
dfs = [pd.read_csv(f) for f in files]
df = pd.concat(dfs, ignore_index=True)

df = df.sort_values("epoch_sec").reset_index(drop=True)

# Compute time gaps between consecutive batches
df["gap_sec"] = df["epoch_sec"].diff()

# Identify the largest gap (Spark downtime)
largest_gap = df.loc[df["gap_sec"].idxmax()]

print("=== Largest gap between batches (Spark downtime) ===")
print(largest_gap)

# Recovery time approximation
print("\n=== Recovery Analysis ===")
print(f"Spark downtime gap (sec): {largest_gap['gap_sec']:.1f}")

# First batch after recovery
recovery_batch = largest_gap
print(f"First recovered batch_id: {int(recovery_batch['batch_id'])}")
print(f"First recovered batch epoch_sec: {recovery_batch['epoch_sec']}")
print(f"p95 latency after recovery (ms): {recovery_batch['p95_latency_ms']}")
