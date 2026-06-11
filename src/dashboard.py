from __future__ import annotations

from pathlib import Path
from typing import Optional

import networkx as nx
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh


st.set_page_config(
    page_title="Projet Big Data - Graphe temps réel",
    page_icon="📊",
    layout="wide",
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "output"


def find_parquet_files(path: Path) -> list[Path]:
    if not path.exists():
        return []

    return [
        file
        for file in path.rglob("*.parquet")
        if file.is_file()
    ]


def read_parquet_folder(path: Path) -> pd.DataFrame:
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


def load_data(output_dir: Path) -> dict[str, pd.DataFrame]:
    vertices = read_first_available(output_dir, ["graph_vertices", "vertices"])
    edges = read_first_available(output_dir, ["graph_edges", "edges"])
    kpis = read_first_available(output_dir, ["dashboard_kpis", "kpis"])
    windows = read_first_available(output_dir, ["windows"])

    return {
        "vertices": vertices,
        "edges": edges,
        "kpis": kpis,
        "windows": windows,
    }


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


def build_graph_figure(
    vertices: pd.DataFrame,
    edges: pd.DataFrame,
    max_edges: int = 80,
) -> Optional[go.Figure]:
    if vertices.empty or edges.empty:
        return None

    required_vertex_cols = {"id", "type", "label"}
    required_edge_cols = {"src", "dst", "relationship"}

    if not required_vertex_cols.issubset(vertices.columns):
        return None

    if not required_edge_cols.issubset(edges.columns):
        return None

    edges_sample = edges.head(max_edges).copy()

    used_node_ids = set(edges_sample["src"].astype(str)) | set(edges_sample["dst"].astype(str))

    vertices_sample = vertices[
        vertices["id"].astype(str).isin(used_node_ids)
    ].copy()

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

    pos = nx.spring_layout(
        graph,
        seed=42,
        k=1.3,
        iterations=80,
    )

    edge_traces = []

    for src, dst, data_edge in graph.edges(data=True):
        x0, y0 = pos[src]
        x1, y1 = pos[dst]
        relationship = data_edge.get("relationship", "")

        edge_traces.append(
            go.Scatter(
                x=[x0, x1, None],
                y=[y0, y1, None],
                mode="lines",
                line=dict(width=1.2, color=edge_color(relationship)),
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

    for node_id, data_node in graph.nodes(data=True):
        x, y = pos[node_id]
        node_x.append(x)
        node_y.append(y)

        node_type = data_node.get("type", "UNKNOWN")
        label = data_node.get("label", node_id)

        node_text.append(
            f"<b>{label}</b><br>"
            f"ID: {node_id}<br>"
            f"Type: {node_type}"
        )

        node_colors.append(node_color(node_type))

        if node_type == "PRODUCT":
            node_sizes.append(22)
        elif node_type == "SELLER":
            node_sizes.append(18)
        else:
            node_sizes.append(14)

    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode="markers",
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
        height=750,
        margin=dict(l=10, r=10, t=40, b=10),
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        title="Graphe dynamique des interactions commerciales",
    )

    return fig


def main() -> None:
    st_autorefresh(interval=5000, key="dashboard_refresh")

    st.title("Graphe temps réel d’interactions commerciales")
    st.caption("Rafraîchissement automatique toutes les 5 secondes")

    st.subheader("Paramètres d'affichage")

    max_edges = st.slider(
        "Nombre maximal d'arêtes affichées",
        min_value=20,
        max_value=200,
        value=100,
        step=20,
    )

    output_dir = DEFAULT_OUTPUT_DIR
    data = load_data(output_dir)

    vertices = data["vertices"]
    edges = data["edges"]
    kpis = data["kpis"]
    windows = data["windows"]

    total_events = 0
    total_achats = 0
    revenue = 0
    unique_users = 0
    unique_products = 0
    unique_sellers = 0

    if not kpis.empty:
        row = kpis.iloc[0]
        total_events = row.get("total_events", 0)
        total_achats = row.get("total_achats", 0)
        revenue = row.get("revenue", 0)
        unique_users = row.get("unique_users", 0)
        unique_products = row.get("unique_products", 0)
        unique_sellers = row.get("unique_sellers", 0)

    col1, col2, col3 = st.columns(3)
    col1.metric("Événements", format_number(total_events))
    col2.metric("Achats", format_number(total_achats))
    col3.metric("Chiffre d'affaires", format_money(revenue))

    col4, col5, col6 = st.columns(3)
    col4.metric("Utilisateurs uniques", format_number(unique_users))
    col5.metric("Produits uniques", format_number(unique_products))
    col6.metric("Vendeurs uniques", format_number(unique_sellers))

    st.header("Vue graphe")

    fig = build_graph_figure(vertices, edges, max_edges=max_edges)

    if fig is None:
        st.info("Aucune donnée de graphe disponible pour le moment.")

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

        st.markdown("### Légende du graphe")

        col_nodes, col_edges = st.columns(2)

        with col_nodes:
            st.markdown(
                """
                **Types de nœuds**

                <span style="color:#4C78A8; font-size:22px;">●</span> **USER** : utilisateur  
                <span style="color:#F58518; font-size:22px;">●</span> **SELLER** : vendeur  
                <span style="color:#54A24B; font-size:22px;">●</span> **PRODUCT** : produit  
                """,
                unsafe_allow_html=True,
            )

        with col_edges:
            st.markdown(
                """
                **Types d’arêtes**

                <span style="color:#8ECAE6; font-size:22px;">━</span> **AIME** : l’utilisateur aime un produit  
                <span style="color:#FFB703; font-size:22px;">━</span> **VOUT** : intention forte d’achat  
                <span style="color:#D62828; font-size:22px;">━</span> **ACHAT** : achat finalisé  
                <span style="color:#6A4C93; font-size:22px;">━</span> **PROPOSE** : le vendeur propose le produit  
                """,
                unsafe_allow_html=True,
            )

    st.header("Fenêtres temporelles Spark")

    if windows.empty:
        st.info("Aucune fenêtre temporelle disponible pour le moment.")
    else:
        st.dataframe(windows.head(20), use_container_width=True)


if __name__ == "__main__":
    main()