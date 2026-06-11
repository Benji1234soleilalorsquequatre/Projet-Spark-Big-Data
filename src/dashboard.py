"""
dashboard.py

Dashboard Streamlit pour visualiser :
- les KPIs du streaming ;
- le graphe dynamique utilisateurs / vendeurs / produits ;
- les fenêtres temporelles Spark ;
- les tops produits, vendeurs et villes.

Version robuste Windows :
- lit les dossiers Parquet produits par Spark ;
- supporte plusieurs noms de dossiers ;
- affiche des messages de debug dans la sidebar ;
- rafraîchissement automatique toutes les 5 secondes.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import networkx as nx
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh


# ---------------------------------------------------------------------
# Configuration générale
# ---------------------------------------------------------------------

st.set_page_config(
    page_title="Projet Big Data - Graphe temps réel",
    page_icon="📊",
    layout="wide",
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "output"


# ---------------------------------------------------------------------
# Fonctions utilitaires
# ---------------------------------------------------------------------

def find_parquet_files(path: Path) -> list[Path]:
    """
    Récupère tous les fichiers parquet dans un dossier Spark.
    Spark écrit souvent :
        output/xxx/part-00000-....snappy.parquet
    """
    if not path.exists():
        return []

    return [
        file
        for file in path.rglob("*.parquet")
        if file.is_file()
    ]


def read_parquet_folder(path: Path) -> pd.DataFrame:
    """
    Lit un dossier Parquet Spark.

    Retourne un DataFrame vide si :
    - le dossier n'existe pas ;
    - aucun fichier parquet n'est présent ;
    - pandas/pyarrow n'arrive pas à lire.
    """
    parquet_files = find_parquet_files(path)

    if not parquet_files:
        return pd.DataFrame()

    dataframes: list[pd.DataFrame] = []

    for file in parquet_files:
        try:
            dataframes.append(pd.read_parquet(file))
        except Exception as exc:
            print(f"Impossible de lire {file}: {exc}")

    if not dataframes:
        return pd.DataFrame()

    try:
        return pd.concat(dataframes, ignore_index=True)
    except Exception:
        return pd.DataFrame()


def read_first_available(output_dir: Path, folder_names: list[str]) -> pd.DataFrame:
    """
    Essaie de lire plusieurs dossiers possibles.
    Utile parce que les noms peuvent varier selon les versions :
    - vertices / graph_vertices
    - edges / graph_edges
    - kpis / dashboard_kpis
    """
    for folder_name in folder_names:
        df = read_parquet_folder(output_dir / folder_name)
        if not df.empty:
            return df

    return pd.DataFrame()


def format_number(value) -> str:
    try:
        return f"{float(value):,.0f}".replace(",", " ")
    except Exception:
        return "0"


def format_money(value) -> str:
    try:
        return f"{float(value):,.2f} €".replace(",", " ")
    except Exception:
        return "0 €"


# ---------------------------------------------------------------------
# Chargement des données
# ---------------------------------------------------------------------

def load_data(output_dir: Path) -> dict[str, pd.DataFrame]:
    """
    Charge toutes les données utiles au dashboard.
    """
    vertices = read_first_available(output_dir, ["graph_vertices", "vertices"])
    edges = read_first_available(output_dir, ["graph_edges", "edges"])
    kpis = read_first_available(output_dir, ["dashboard_kpis", "kpis"])

    windows = read_first_available(output_dir, ["windows"])
    top_products = read_first_available(output_dir, ["top_products"])
    top_sellers = read_first_available(output_dir, ["top_sellers"])
    top_cities = read_first_available(output_dir, ["top_cities"])
    degrees = read_first_available(output_dir, ["graph_metrics", "degrees"])

    return {
        "vertices": vertices,
        "edges": edges,
        "kpis": kpis,
        "windows": windows,
        "top_products": top_products,
        "top_sellers": top_sellers,
        "top_cities": top_cities,
        "degrees": degrees,
    }


# ---------------------------------------------------------------------
# Graphe Plotly
# ---------------------------------------------------------------------

def node_color(node_type: str) -> str:
    if node_type == "USER":
        return "#4C78A8"
    if node_type == "SELLER":
        return "#F58518"
    if node_type == "PRODUCT":
        return "#54A24B"
    return "#999999"


def edge_color(relationship: str) -> str:
    if relationship == "AIME":
        return "#8ECAE6"
    if relationship == "VOUT":
        return "#FFB703"
    if relationship == "ACHAT":
        return "#D62828"
    if relationship == "PROPOSE":
        return "#6A4C93"
    return "#999999"


def build_graph_figure(vertices: pd.DataFrame, edges: pd.DataFrame, max_edges: int = 200) -> Optional[go.Figure]:
    """
    Construit un graphe NetworkX puis l'affiche avec Plotly.
    """

    if vertices.empty or edges.empty:
        return None

    required_vertex_cols = {"id", "type", "label"}
    required_edge_cols = {"src", "dst", "relationship"}

    if not required_vertex_cols.issubset(vertices.columns):
        return None

    if not required_edge_cols.issubset(edges.columns):
        return None

    # Limiter le nombre d'arêtes pour garder un dashboard fluide.
    edges_sample = edges.head(max_edges).copy()

    used_node_ids = set(edges_sample["src"].astype(str)) | set(edges_sample["dst"].astype(str))
    vertices_sample = vertices[vertices["id"].astype(str).isin(used_node_ids)].copy()

    if vertices_sample.empty or edges_sample.empty:
        return None

    graph = nx.DiGraph()

    for _, row in vertices_sample.iterrows():
        node_id = str(row["id"])
        graph.add_node(
            node_id,
            label=str(row.get("label", node_id)),
            type=str(row.get("type", "UNKNOWN")),
        )

    for _, row in edges_sample.iterrows():
        src = str(row["src"])
        dst = str(row["dst"])
        relationship = str(row["relationship"])

        if src in graph.nodes and dst in graph.nodes:
            graph.add_edge(src, dst, relationship=relationship)

    if graph.number_of_nodes() == 0:
        return None

    pos = nx.spring_layout(graph, seed=42, k=0.8)

    edge_traces = []

    for src, dst, data in graph.edges(data=True):
        x0, y0 = pos[src]
        x1, y1 = pos[dst]
        relationship = data.get("relationship", "")

        edge_traces.append(
            go.Scatter(
                x=[x0, x1, None],
                y=[y0, y1, None],
                mode="lines",
                line=dict(width=1.5, color=edge_color(relationship)),
                hoverinfo="text",
                text=f"{src} → {dst}<br>{relationship}",
                showlegend=False,
            )
        )

    node_x = []
    node_y = []
    node_text = []
    node_colors = []
    node_sizes = []

    degrees = dict(graph.degree())

    for node_id, data in graph.nodes(data=True):
        x, y = pos[node_id]
        node_x.append(x)
        node_y.append(y)

        node_type = data.get("type", "UNKNOWN")
        label = data.get("label", node_id)
        degree = degrees.get(node_id, 1)

        node_text.append(
            f"<b>{label}</b><br>"
            f"ID: {node_id}<br>"
            f"Type: {node_type}<br>"
            f"Degré: {degree}"
        )
        node_colors.append(node_color(node_type))
        node_sizes.append(12 + min(degree * 2, 25))

    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode="markers+text",
        text=[str(node)[:12] for node in graph.nodes()],
        textposition="top center",
        hoverinfo="text",
        hovertext=node_text,
        marker=dict(
            size=node_sizes,
            color=node_colors,
            line=dict(width=1, color="#333333"),
        ),
        showlegend=False,
    )

    fig = go.Figure(data=edge_traces + [node_trace])

    fig.update_layout(
        height=650,
        margin=dict(l=10, r=10, t=30, b=10),
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        title="Graphe dynamique des interactions commerciales",
    )

    return fig


# ---------------------------------------------------------------------
# Interface Streamlit
# ---------------------------------------------------------------------

def main() -> None:
    st_autorefresh(interval=5000, key="dashboard_refresh")

    st.title("Projet Big Data — Graphe temps réel d’interactions commerciales")
    st.caption("Rafraîchissement automatique toutes les 5 secondes")

    with st.sidebar:
        st.header("Configuration")
        output_dir_str = st.text_input("Dossier output", value=str(DEFAULT_OUTPUT_DIR))
        output_dir = Path(output_dir_str)

        st.write("Dossier détecté :")
        st.code(str(output_dir))

        st.write("Existe :", output_dir.exists())

        if output_dir.exists():
            folders = sorted([p.name for p in output_dir.iterdir() if p.is_dir()])
            st.write("Sous-dossiers détectés :")
            st.write(folders)

        max_edges = st.slider("Nombre maximal d'arêtes affichées", 20, 1000, 200, step=20)

    data = load_data(output_dir)

    vertices = data["vertices"]
    edges = data["edges"]
    kpis = data["kpis"]
    windows = data["windows"]
    top_products = data["top_products"]
    top_sellers = data["top_sellers"]
    top_cities = data["top_cities"]
    degrees = data["degrees"]

    with st.sidebar:
        st.header("Debug lecture")
        st.write("vertices :", vertices.shape)
        st.write("edges :", edges.shape)
        st.write("kpis :", kpis.shape)
        st.write("windows :", windows.shape)
        st.write("top_products :", top_products.shape)
        st.write("top_sellers :", top_sellers.shape)
        st.write("top_cities :", top_cities.shape)

    # -----------------------------------------------------------------
    # KPIs
    # -----------------------------------------------------------------

    total_events = 0
    total_achats = 0
    revenue = 0
    batch_value = "-"

    if not kpis.empty:
        row = kpis.iloc[0]
        total_events = row.get("total_events", 0)
        total_achats = row.get("total_achats", 0)
        revenue = row.get("revenue", 0)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Événements", format_number(total_events))
    col2.metric("Achats", format_number(total_achats))
    col3.metric("CA", format_money(revenue))
    col4.metric("Batch", batch_value)

    # -----------------------------------------------------------------
    # Graphe
    # -----------------------------------------------------------------

    st.header("Vue graphe")

    fig = build_graph_figure(vertices, edges, max_edges=max_edges)

    if fig is None:
        st.info("Aucune donnée de graphe disponible. Vérifie que Spark écrit bien graph_vertices et graph_edges.")

        with st.expander("Debug graphe"):
            st.write("Vertices shape :", vertices.shape)
            st.write("Edges shape :", edges.shape)

            if not vertices.empty:
                st.write("Colonnes vertices :", list(vertices.columns))
                st.dataframe(vertices.head(10), use_container_width=True)

            if not edges.empty:
                st.write("Colonnes edges :", list(edges.columns))
                st.dataframe(edges.head(10), use_container_width=True)

    else:
        st.plotly_chart(fig, use_container_width=True)

        st.caption(
            "Couleurs des nœuds : USER = bleu, SELLER = orange, PRODUCT = vert. "
            "Les arêtes représentent AIME, VOUT, ACHAT ou PROPOSE."
        )

    # -----------------------------------------------------------------
    # Fenêtres temporelles
    # -----------------------------------------------------------------

    st.header("Fenêtres temporelles Spark")

    if windows.empty:
        st.info("Aucune fenêtre temporelle disponible pour le moment.")
    else:
        st.dataframe(windows.head(20), use_container_width=True)

    # -----------------------------------------------------------------
    # Tops
    # -----------------------------------------------------------------

    col_a, col_b, col_c = st.columns(3)

    with col_a:
        st.header("Top produits")
        if top_products.empty:
            st.info("Aucune donnée produit.")
        else:
            st.dataframe(top_products.head(10), use_container_width=True)

    with col_b:
        st.header("Top vendeurs")
        if top_sellers.empty:
            st.info("Aucune donnée vendeur.")
        else:
            st.dataframe(top_sellers.head(10), use_container_width=True)

    with col_c:
        st.header("Top villes")
        if top_cities.empty:
            st.info("Aucune donnée ville.")
        else:
            st.dataframe(top_cities.head(10), use_container_width=True)

    # -----------------------------------------------------------------
    # Métriques de graphe
    # -----------------------------------------------------------------

    st.header("Métriques de graphe")

    if degrees.empty:
        st.info("Aucune métrique de degré disponible.")
    else:
        st.dataframe(degrees.head(20), use_container_width=True)


if __name__ == "__main__":
    main()