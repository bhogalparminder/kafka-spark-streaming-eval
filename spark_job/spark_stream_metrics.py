import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("created_ts", LongType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("payload", StringType(), True),
])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", default="localhost:9092")
    parser.add_argument("--topic", default="events_topic")
    parser.add_argument("--checkpoint", default="/tmp/spark_kafka_checkpoint")
    parser.add_argument("--outdir", default="file:///home/$(whoami)/paper_project/results/metrics")
    parser.add_argument("--trigger", default="5 seconds", help='e.g. "5 seconds"')
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("KafkaSparkLatencyMetrics")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read stream from Kafka
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.brokers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .load()
    )

    # Kafka value is bytes -> string -> JSON
    json_df = raw.selectExpr("CAST(value AS STRING) as value_str")

    parsed = json_df.select(from_json(col("value_str"), EVENT_SCHEMA).alias("e")).select("e.*")

    # Compute latency in ms using current timestamp (processing-time proxy)
    # created_ts is ms since epoch from producer
    with_latency = parsed.withColumn(
        "latency_ms",
        (expr("unix_timestamp(current_timestamp()) * 1000") - col("created_ts")).cast("long")
    )

    def write_batch(batch_df, batch_id: int):
        # Skip empty batches
        if batch_df.rdd.isEmpty():
            return

        n = batch_df.count()
        # Approx percentiles for large streams
        p50, p95, p99 = batch_df.approxQuantile("latency_ms", [0.50, 0.95, 0.99], 0.01)
        mean_latency = batch_df.agg(expr("avg(latency_ms) as mean")).collect()[0]["mean"]

        now = time.time()
        record = [(batch_id, int(now), n, float(mean_latency), float(p50), float(p95), float(p99))]

        out_schema = "batch_id long, epoch_sec long, batch_events long, mean_latency_ms double, p50_latency_ms double, p95_latency_ms double, p99_latency_ms double"
        out_df = spark.createDataFrame(record, schema=out_schema)

        # Append as a single CSV row per batch
        (out_df.coalesce(1)
              .write.mode("append")
              .option("header", "true")
              .csv(args.outdir))

        print(f"[batch {batch_id}] events={n} mean={mean_latency:.1f}ms p50={p50:.1f} p95={p95:.1f} p99={p99:.1f}")

    query = (
        with_latency.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger)
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()
