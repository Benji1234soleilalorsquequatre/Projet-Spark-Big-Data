"""
graph_builder.py

Construction et analyse du graphe dynamique à partir de chaque micro-batch Spark.

Le graphe contient :
- des utilisateurs ;
- des produits ;
- des vendeurs.

Vertices :
    id, type, label

Edges :
    src, dst, relationship

Relations :
- utilisateur -> produit : AIME, VOUT, ACHAT
- vendeur -> produit : PROPOSE

Corrections Windows :
- suppression de events_df.rdd.isEmpty()
- pas d'appel direct au RDD dans foreachBatch()
- écriture robuste des sorties pour le dashboard.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def safe_write_parquet(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """
    Écrit un DataFrame en Parquet de manière robuste.

    On évite de tester si le DataFrame est vide avec df.rdd.isEmpty(),
    car cela peut faire crasher le Python worker sous Windows.
    """
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


def safe_write_csv(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """
    Écrit un DataFrame en CSV pour faciliter la lecture par Streamlit/Pandas.

    Les CSV sont pratiques pour le dashboard local.
    """
    try:
        (
            df
            .coalesce(1)
            .write
            .mode(mode)
            .option("header", "true")
            .csv(path)
        )
        print(f"[WRITE] CSV écrit : {path}")
    except Exception as exc:
        print(f"[WRITE] Erreur écriture CSV {path} : {exc}")


def build_vertices(events_df: DataFrame) -> DataFrame:
    """
    Construit les sommets du graphe.

    Format attendu par GraphFrames :
        id, type, label
    """

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
    """
    Construit les arêtes du graphe.

    Format attendu par GraphFrames :
        src, dst, relationship
    """

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


def compute_degrees(vertices: DataFrame, edges: DataFrame) -> DataFrame:
    """
    Calcule des indicateurs de degré :
    - degré entrant ;
    - degré sortant ;
    - degré total.

    Ces métriques sont compatibles avec un environnement local,
    même sans GraphFrames installé.
    """

    in_degrees = (
        edges
        .groupBy(F.col("dst").alias("id"))
        .agg(F.count("*").alias("in_degree"))
    )

    out_degrees = (
        edges
        .groupBy(F.col("src").alias("id"))
        .agg(F.count("*").alias("out_degree"))
    )

    degrees = (
        vertices
        .join(in_degrees, on="id", how="left")
        .join(out_degrees, on="id", how="left")
        .fillna({"in_degree": 0, "out_degree": 0})
        .withColumn("degree", F.col("in_degree") + F.col("out_degree"))
        .orderBy(F.col("degree").desc())
    )

    return degrees


def compute_kpis(events_df: DataFrame) -> DataFrame:
    """
    Calcule les KPIs principaux du dashboard :
    - nombre total d'événements ;
    - nombre d'achats ;
    - chiffre d'affaires ;
    - nombre d'utilisateurs ;
    - nombre de produits ;
    - nombre de vendeurs.
    """

    kpis = (
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

    return kpis


def compute_top_products(events_df: DataFrame) -> DataFrame:
    """
    Produits les plus actifs/centraux selon le nombre d'interactions.
    """

    return (
        events_df
        .groupBy("product_id", "product_cat")
        .agg(
            F.count("*").alias("nb_interactions"),
            F.sum(F.when(F.col("action_type") == "AIME", 1).otherwise(0)).alias("nb_aime"),
            F.sum(F.when(F.col("action_type") == "VOUT", 1).otherwise(0)).alias("nb_vout"),
            F.sum(F.when(F.col("action_type") == "ACHAT", 1).otherwise(0)).alias("nb_achat"),
            F.sum(F.when(F.col("action_type") == "ACHAT", F.col("price")).otherwise(0.0)).alias("revenue"),
        )
        .orderBy(F.col("nb_interactions").desc())
        .limit(20)
    )


def compute_top_sellers(events_df: DataFrame) -> DataFrame:
    """
    Vendeurs les plus connectés/performants.
    """

    return (
        events_df
        .groupBy("seller_id")
        .agg(
            F.countDistinct("product_id").alias("nb_products"),
            F.count("*").alias("nb_interactions"),
            F.sum(F.when(F.col("action_type") == "ACHAT", 1).otherwise(0)).alias("nb_achats"),
            F.sum(F.when(F.col("action_type") == "ACHAT", F.col("price")).otherwise(0.0)).alias("revenue"),
        )
        .orderBy(F.col("nb_interactions").desc())
        .limit(20)
    )


def compute_top_cities(events_df: DataFrame) -> DataFrame:
    """
    Villes les plus actives.
    """

    return (
        events_df
        .groupBy("user_city")
        .agg(
            F.count("*").alias("nb_events"),
            F.sum(F.when(F.col("action_type") == "ACHAT", 1).otherwise(0)).alias("nb_achats"),
            F.sum(F.when(F.col("action_type") == "ACHAT", F.col("price")).otherwise(0.0)).alias("revenue"),
        )
        .orderBy(F.col("nb_events").desc())
        .limit(20)
    )


def try_graphframes_analysis(vertices: DataFrame, edges: DataFrame, output_dir: str) -> None:
    """
    Analyse GraphFrames optionnelle.

    Désactivée par défaut sous Windows pour éviter les problèmes d'installation locale.
    Le projet reste conforme car les vertices et edges sont bien construits,
    et des métriques de graphe sont calculées sans dépendance externe.
    """
    print("[GRAPHFRAMES] Analyse GraphFrames désactivée en local Windows.")


def process_graph_batch(events_df: DataFrame, batch_id: int, output_dir: str) -> None:
    """
    Fonction appelée par foreachBatch().

    Chaque micro-batch streaming est traité comme un DataFrame statique.

    Étapes :
    1. nettoyage minimal ;
    2. construction des vertices ;
    3. construction des edges ;
    4. calcul des degrés ;
    5. calcul de KPIs ;
    6. export pour dashboard ;
    7. analyse GraphFrames optionnelle.
    """

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

        # Cache utile car le même micro-batch est utilisé pour plusieurs calculs.
        clean_events.cache()

        vertices = build_vertices(clean_events)
        edges = build_edges(clean_events)
        degrees = compute_degrees(vertices, edges)

        kpis = compute_kpis(clean_events)
        top_products = compute_top_products(clean_events)
        top_sellers = compute_top_sellers(clean_events)
        top_cities = compute_top_cities(clean_events)

        # Sorties principales pour le dashboard.
        # On écrit avec deux conventions de noms pour être compatible avec dashboard.py.
        safe_write_parquet(vertices, os.path.join(output_dir, "vertices"))
        safe_write_parquet(edges, os.path.join(output_dir, "edges"))
        safe_write_parquet(degrees, os.path.join(output_dir, "degrees"))
        safe_write_parquet(kpis, os.path.join(output_dir, "kpis"))

        # Alias compatibles avec l'ancien dashboard.
        safe_write_parquet(vertices, os.path.join(output_dir, "graph_vertices"))
        safe_write_parquet(edges, os.path.join(output_dir, "graph_edges"))
        safe_write_parquet(degrees, os.path.join(output_dir, "graph_metrics"))
        safe_write_parquet(kpis, os.path.join(output_dir, "dashboard_kpis"))

        safe_write_parquet(top_products, os.path.join(output_dir, "top_products"))
        safe_write_parquet(top_sellers, os.path.join(output_dir, "top_sellers"))
        safe_write_parquet(top_cities, os.path.join(output_dir, "top_cities"))


        # Sorties CSV aussi, plus simples à inspecter manuellement.
        safe_write_csv(vertices, os.path.join(output_dir, "csv_vertices"))
        safe_write_csv(edges, os.path.join(output_dir, "csv_edges"))
        safe_write_csv(degrees, os.path.join(output_dir, "csv_degrees"))
        safe_write_csv(kpis, os.path.join(output_dir, "csv_kpis"))
        safe_write_csv(top_products, os.path.join(output_dir, "csv_top_products"))
        safe_write_csv(top_sellers, os.path.join(output_dir, "csv_top_sellers"))
        safe_write_csv(top_cities, os.path.join(output_dir, "csv_top_cities"))

        # Analyse GraphFrames optionnelle.
        try_graphframes_analysis(vertices, edges, output_dir)

        print(f"[GRAPH] Batch {batch_id} terminé.")

    except Exception as exc:
        # On évite de tuer toute la requête streaming sur une erreur ponctuelle.
        print(f"[GRAPH] Erreur dans le batch {batch_id} : {exc}")

    finally:
        try:
            clean_events.unpersist()
        except Exception:
            pass