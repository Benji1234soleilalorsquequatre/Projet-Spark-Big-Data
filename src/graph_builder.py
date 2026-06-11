from __future__ import annotations

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def safe_write_parquet(df: DataFrame, path: str, mode: str = "overwrite") -> None:

    try:
        (
            df
            .coalesce(1)
            .write
            .mode(mode)
            .parquet(path)
        )
        print(f"[WRITE] Parquet écrit : {path}")
    except Exception as exc:
        print(f"[WRITE] Erreur écriture Parquet {path} : {exc}")


def build_vertices(events_df: DataFrame) -> DataFrame:

    users = (
        events_df
        .select(
            F.col("user_id").alias("id"),
            F.lit("USER").alias("type"),
            F.col("user_id").alias("label"),
        )
    )

    products = (
        events_df
        .select(
            F.col("product_id").alias("id"),
            F.lit("PRODUCT").alias("type"),
            F.concat_ws(" - ", F.col("product_id"), F.col("product_cat")).alias("label"),
        )
    )

    sellers = (
        events_df
        .select(
            F.col("seller_id").alias("id"),
            F.lit("SELLER").alias("type"),
            F.col("seller_id").alias("label"),
        )
    )

    vertices = (
        users
        .unionByName(products)
        .unionByName(sellers)
        .dropDuplicates(["id"])
    )

    return vertices


def build_edges(events_df: DataFrame) -> DataFrame:

    user_product_edges = (
        events_df
        .select(
            F.col("user_id").alias("src"),
            F.col("product_id").alias("dst"),
            F.col("action_type").alias("relationship"),
        )
    )

    seller_product_edges = (
        events_df
        .select(
            F.col("seller_id").alias("src"),
            F.col("product_id").alias("dst"),
            F.lit("PROPOSE").alias("relationship"),
        )
    )

    edges = (
        user_product_edges
        .unionByName(seller_product_edges)
        .dropDuplicates(["src", "dst", "relationship"])
    )

    return edges


def compute_kpis(events_df: DataFrame) -> DataFrame:

    return (
        events_df
        .agg(
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("action_type") == "ACHAT", 1).otherwise(0)).alias("total_achats"),
            F.sum(F.when(F.col("action_type") == "ACHAT", F.col("price")).otherwise(0.0)).alias("revenue"),
            F.countDistinct("user_id").alias("unique_users"),
            F.countDistinct("product_id").alias("unique_products"),
            F.countDistinct("seller_id").alias("unique_sellers"),
        )
    )


def process_graph_batch(events_df: DataFrame, batch_id: int, output_dir: str) -> None:

    print(f"[GRAPH] Début traitement batch {batch_id}")

    try:
        clean_events = (
            events_df
            .filter(F.col("user_id").isNotNull())
            .filter(F.col("product_id").isNotNull())
            .filter(F.col("seller_id").isNotNull())
            .filter(F.col("action_type").isin("AIME", "VOUT", "ACHAT"))
            .select(
                "timestamp",
                "timestamp_ts",
                "user_id",
                "user_city",
                "product_id",
                "product_cat",
                "seller_id",
                "action_type",
                "price",
            )
        )

        clean_events.cache()

        vertices = build_vertices(clean_events)
        edges = build_edges(clean_events)
        kpis = compute_kpis(clean_events)

        # Noms simples.
        safe_write_parquet(vertices, os.path.join(output_dir, "vertices"))
        safe_write_parquet(edges, os.path.join(output_dir, "edges"))
        safe_write_parquet(kpis, os.path.join(output_dir, "kpis"))

        # Alias utilisés par le dashboard.
        safe_write_parquet(vertices, os.path.join(output_dir, "graph_vertices"))
        safe_write_parquet(edges, os.path.join(output_dir, "graph_edges"))
        safe_write_parquet(kpis, os.path.join(output_dir, "dashboard_kpis"))

        print(f"[GRAPH] Batch {batch_id} terminé.")

    except Exception as exc:
        print(f"[GRAPH] Erreur dans le batch {batch_id} : {exc}")

    finally:
        try:
            clean_events.unpersist()
        except Exception:
            pass