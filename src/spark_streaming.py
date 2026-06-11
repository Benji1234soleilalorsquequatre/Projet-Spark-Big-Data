from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from graph_builder import process_graph_batch


def create_spark(app_name: str, shuffle_partitions: int, driver_memory: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.host", "127.0.0.1")
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
        )
        .getOrCreate()
    )

    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    print(f"SparkContext: appId={sc.applicationId}, master={sc.master}")

    return spark


def event_schema() -> StructType:
    return StructType(
        [
            StructField("timestamp", StringType(), nullable=False),
            StructField("user_id", StringType(), nullable=False),
            StructField("user_city", StringType(), nullable=False),
            StructField("product_id", StringType(), nullable=False),
            StructField("product_cat", StringType(), nullable=False),
            StructField("seller_id", StringType(), nullable=False),
            StructField("action_type", StringType(), nullable=False),
            StructField("price", DoubleType(), nullable=False),
        ]
    )


def write_windows_batch(batch_df: DataFrame, batch_id: int, output_dir: str) -> None:
    windows_output_path = os.path.join(output_dir, "windows")

    try:
        (
            batch_df
            .orderBy(F.col("window_start").desc())
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(windows_output_path)
        )

        print(f"[WINDOWS] Batch {batch_id} écrit dans {windows_output_path}")

    except Exception as exc:
        print(f"[WINDOWS] Batch {batch_id} ignoré ou erreur d'écriture : {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Structured Streaming LeBonCoin Graph Project")
    parser.add_argument("--input-dir", default="data/input_json")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--checkpoint-dir", default="checkpoints")
    parser.add_argument("--shuffle-partitions", type=int, default=4)
    parser.add_argument("--driver-memory", default="2g")
    args = parser.parse_args()

    os.makedirs(args.input_dir, exist_ok=True)
    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(args.checkpoint_dir, exist_ok=True)

    spark = create_spark(
        app_name="LeboncoinStreamingGraph",
        shuffle_partitions=args.shuffle_partitions,
        driver_memory=args.driver_memory,
    )

    raw = (
        spark.readStream
        .schema(event_schema())
        .option("maxFilesPerTrigger", 5)
        .json(args.input_dir)
    )

    events = (
        raw
        .withColumn("timestamp_ts", F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ssX"))
        .filter(F.col("timestamp_ts").isNotNull())
        .filter(F.col("action_type").isin("AIME", "VOUT", "ACHAT"))
    )

    watermarked = events.withWatermark("timestamp_ts", "2 minutes")

    windowed = (
        watermarked
        .groupBy(F.window("timestamp_ts", "1 minute", "30 seconds"))
        .agg(
            F.count("*").alias("nb_events"),
            F.sum(F.when(F.col("action_type") == "AIME", 1).otherwise(0)).alias("nb_aime"),
            F.sum(F.when(F.col("action_type") == "VOUT", 1).otherwise(0)).alias("nb_vout"),
            F.sum(F.when(F.col("action_type") == "ACHAT", 1).otherwise(0)).alias("nb_achat"),
            F.sum(F.when(F.col("action_type") == "ACHAT", F.col("price")).otherwise(0.0)).alias("revenue"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "nb_events",
            "nb_aime",
            "nb_vout",
            "nb_achat",
            "revenue",
        )
    )

    query_windows = (
        windowed.writeStream
        .outputMode("update")
        .foreachBatch(lambda df, batch_id: write_windows_batch(df, batch_id, args.output_dir))
        .option("checkpointLocation", f"{args.checkpoint_dir}/windows")
        .trigger(processingTime="5 seconds")
        .start()
    )

    query_graph = (
        watermarked.writeStream
        .outputMode("append")
        .foreachBatch(lambda df, batch_id: process_graph_batch(df, batch_id, args.output_dir))
        .option("checkpointLocation", f"{args.checkpoint_dir}/graph")
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("Streaming lancé.")
    print("Dashboard Spark UI : http://127.0.0.1:4040")
    print("Arrêt : Ctrl + C")

    spark.streams.awaitAnyTermination()

    query_windows.awaitTermination()
    query_graph.awaitTermination()


if __name__ == "__main__":
    main()