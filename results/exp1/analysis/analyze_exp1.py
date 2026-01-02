import glob
import os
import re
import pandas as pd
import matplotlib.pyplot as plt

EXP1_DIR = os.path.expanduser("~/paper_project/results/exp1")
OUT_DIR = os.path.join(EXP1_DIR, "analysis")

TRIGGER_SEC = 5                # you confirmed 5 seconds
WARMUP_SEC = 120               # ignore first 2 minutes
MEASURE_SEC = 240              # use next 4 minutes

def read_rate_folder(rate_folder: str) -> pd.DataFrame:
    # Spark writes many small CSV files inside nested folders. Grab all *.csv recursively.
    csv_files = glob.glob(os.path.join(rate_folder, "**", "*.csv"), recursive=True)
    if not csv_files:
        raise RuntimeError(f"No CSV files found under: {rate_folder}")

    dfs = []
    for f in csv_files:
        try:
            df = pd.read_csv(f)
            dfs.append(df)
        except Exception:
            # skip unreadable files safely
            pass

    if not dfs:
        raise RuntimeError(f"Could not read any CSV files under: {rate_folder}")

    df = pd.concat(dfs, ignore_index=True)

    # Keep only expected columns (defensive)
    expected_cols = [
        "batch_id", "epoch_sec", "batch_events",
        "mean_latency_ms", "p50_latency_ms", "p95_latency_ms", "p99_latency_ms"
    ]
    df = df[[c for c in expected_cols if c in df.columns]].copy()

    # Convert to numeric, drop bad rows
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["epoch_sec", "batch_events", "p95_latency_ms"])

    # Sort by time
    df = df.sort_values("epoch_sec").reset_index(drop=True)

    # Compute throughput per batch (events/sec)
    df["throughput_eps"] = df["batch_events"] / TRIGGER_SEC
    return df

def window_measurement(df: pd.DataFrame) -> pd.DataFrame:
    """
    Use a fixed window per run:
      - warm-up: first 120 seconds excluded
      - measurement: next 240 seconds included
    """
    start = df["epoch_sec"].min()
    t0 = start + WARMUP_SEC
    t1 = t0 + MEASURE_SEC
    measured = df[(df["epoch_sec"] >= t0) & (df["epoch_sec"] < t1)].copy()

    # If run is shorter than expected, fall back to "after warm-up"
    if len(measured) < 5:
        measured = df[df["epoch_sec"] >= t0].copy()

    return measured

def summarize_rate(rate: int, df_meas: pd.DataFrame) -> dict:
    return {
        "rate_eps_input": rate,
        "batches_used": int(len(df_meas)),
        "throughput_eps_mean": float(df_meas["throughput_eps"].mean()),
        "throughput_eps_p95": float(df_meas["throughput_eps"].quantile(0.95)),
        "lat_mean_ms_avg": float(df_meas["mean_latency_ms"].mean()),
        "lat_p50_ms_avg": float(df_meas["p50_latency_ms"].mean()),
        "lat_p95_ms_avg": float(df_meas["p95_latency_ms"].mean()),
        "lat_p99_ms_avg": float(df_meas["p99_latency_ms"].mean()),
        "lat_p95_ms_max": float(df_meas["p95_latency_ms"].max()),
    }

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    # Find rate folders like rate_7000
    folders = sorted([d for d in os.listdir(EXP1_DIR) if d.startswith("rate_")])
    if not folders:
        raise RuntimeError(f"No rate folders found in {EXP1_DIR}")

    summaries = []
    per_rate_series = {}

    for folder in folders:
        m = re.match(r"rate_(\d+)", folder)
        if not m:
            continue
        rate = int(m.group(1))
        path = os.path.join(EXP1_DIR, folder)

        df = read_rate_folder(path)
        df_meas = window_measurement(df)

        summaries.append(summarize_rate(rate, df_meas))

        # Save per-rate measurement data (useful for debugging/paper appendix)
        per_rate_series[rate] = df_meas

    summary_df = pd.DataFrame(summaries).sort_values("rate_eps_input")
    out_csv = os.path.join(OUT_DIR, "exp1_summary.csv")
    summary_df.to_csv(out_csv, index=False)
    print(f"Wrote summary table: {out_csv}")
    print(summary_df)

    # Plot 1: throughput vs input rate
    plt.figure()
    plt.plot(summary_df["rate_eps_input"], summary_df["throughput_eps_mean"], marker="o")
    plt.xlabel("Input rate (events/sec)")
    plt.ylabel("Mean throughput (events/sec)")
    plt.title("Experiment 1: Throughput vs Input Rate")
    plt.grid(True, linestyle="--", linewidth=0.5)
    out1 = os.path.join(OUT_DIR, "throughput_vs_rate.png")
    plt.savefig(out1, dpi=200, bbox_inches="tight")
    print(f"Saved plot: {out1}")

    # Plot 2: p95 latency vs input rate
    plt.figure()
    plt.plot(summary_df["rate_eps_input"], summary_df["lat_p95_ms_avg"], marker="o")
    plt.xlabel("Input rate (events/sec)")
    plt.ylabel("Average p95 end-to-end latency (ms)")
    plt.title("Experiment 1: p95 Latency vs Input Rate")
    plt.grid(True, linestyle="--", linewidth=0.5)
    out2 = os.path.join(OUT_DIR, "p95_latency_vs_rate.png")
    plt.savefig(out2, dpi=200, bbox_inches="tight")
    print(f"Saved plot: {out2}")

if __name__ == "__main__":
    main()
