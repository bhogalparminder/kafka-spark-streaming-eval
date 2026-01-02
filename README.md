# Kafka–Spark Streaming Experiment Reproducibility

This repository contains the code and analysis used in the paper:

**“Scalability and Fault-Tolerance Evaluation of a Kafka–Spark Streaming Pipeline.”**

It enables reproduction of three experiments:
1. Throughput and latency under increasing load
2. Scalability via Kafka partitions and Spark parallelism
3. Fault tolerance and recovery after Spark failure

---

## Repository Structure
```yaml
kafka-spark-streaming-eval/
├── producer/ # Kafka event producer
│ └── producer.py
├── spark_job/ # Spark Structured Streaming job
│ └── spark_stream_metrics.py
├── results/
│ ├── metrics/ # Spark writes per-batch CSVs here
│ ├── exp1/
│ │ └── analysis/analyze_exp1.py
│ ├── exp2/
│ │ └── analysis/analyze_exp2.py
│ ├── exp3/
│ └── exp3_timestamps.txt
├── requirements.txt
├── figures/ # System Architecture Diagram
└── README.md
```

Raw per-batch CSV outputs are not tracked in git.

---

## Prerequisites

- Linux
- Java 11+
- Python 3.9+
- Apache Kafka 3.x
- Apache Spark 3.5.x

Install Python dependencies:

```bash
pip install -r requirements.txt
```

## Running the Streaming Pipeline
Start Kafka

Create a topic (example with 3 partitions):

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic events_topic \
  --partitions 3 --replication-factor 1
```

## Start Spark Streaming

From the repository root:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --master local[4] \
  --driver-memory 8g \
  spark_job/spark_stream_metrics.py \
  --brokers localhost:9092 \
  --topic events_topic \
  --checkpoint /tmp/spark_kafka_checkpoint \
  --outdir file:///home/$USER/kafka-spark-streaming-eval/results/metrics \
  --trigger "5 seconds"
```

## Run the Producer

In a separate terminal:

```bash
python3 producer/producer.py \
  --rate 7000 \
  --minutes 6 \
  --brokers localhost:9092 \
  --topic events_topic
```

## Handling Metrics Output (Important)

Spark writes batch-level metric CSVs to:

```bash
results/metrics/
```

Before each run:

```bash
rm -rf results/metrics/*
```


After each run, copy metrics into the appropriate experiment folder:

Experiment 1 (example):

```bash
cp -r results/metrics/* results/exp1/rate_7000/
```


Experiment 2 (example):

```bash
cp -r results/metrics/* results/exp2/kafka_partitions_12/
```

Running Analysis

Experiment 1:

```bash
python3 results/exp1/analysis/analyze_exp1.py
```


Experiment 2:

```bash
python3 results/exp2/analysis/analyze_exp2.py
```


Experiment 3:
Use the recovery workflow described in the paper and analyze results with your recovery scripts.

## Notes

- Experiments were run on a single-node setup to isolate ingestion vs compute bottlenecks.
- Results may vary with hardware and configuration.
- This repository tracks code and analysis only; large raw outputs are intentionally excluded.