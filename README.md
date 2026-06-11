# Projet d’Ingénierie Big Data & Analyse de Graphes Temps Réel

Plateforme de streaming infini d’interactions commerciales inspirée de LeBonCoin. Le projet simule des événements utilisateur en JSON, les traite avec PySpark Structured Streaming, construit un graphe dynamique avec GraphFrames via `foreachBatch()` et visualise le résultat dans un dashboard Streamlit.

## Architecture

```text
Simulateur Python JSON
        ↓ fichiers JSON Lines atomiques
Dossier data/input_json surveillé par Spark
        ↓ readStream + schéma strict
PySpark Structured Streaming
        ↓ watermark + fenêtres glissantes
Agrégations temporelles + KPIs
        ↓ foreachBatch
GraphFrames / métriques de graphe
        ↓ CSV / Parquet dans output/
Dashboard Streamlit rafraîchi toutes les 5 s
```

## Structure

```text
spark-bigdata-graph-project/
├── README.md
├── requirements.txt
├── docker-compose.yml
├── data/input_json/              # fichiers JSON générés en continu
├── src/
│   ├── producer.py               # générateur de données infini
│   ├── spark_streaming.py         # pipeline Structured Streaming
│   ├── graph_builder.py           # construction vertices/edges + métriques
│   └── dashboard.py               # interface Streamlit dynamique
├── output/                        # sorties Spark lues par le dashboard
├── checkpoints/                   # checkpoints Spark Streaming
└── docs/
```

## Prérequis

- Python 3.10 ou 3.11
- Java 8/11/17 compatible Spark
- Apache Spark 3.5.x ou `pyspark` installé via pip
- Optionnel : GraphFrames pour PageRank réel

Installation locale :

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
```

## Lancement

Terminal 1 — générateur :

```bash
python src/producer.py --output-dir data/input_json --batch-size 50 --sleep 2
```

Terminal 2 — Spark Streaming :

```bash
spark-submit src/spark_streaming.py \
  --input-dir data/input_json \
  --output-dir output \
  --checkpoint-dir checkpoints \
  --shuffle-partitions 4 \
  --driver-memory 2g
```

Avec GraphFrames, adapter selon la version Spark/Scala disponible :

```bash
spark-submit \
  --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 \
  src/spark_streaming.py
```

Terminal 3 — dashboard :

```bash
streamlit run src/dashboard.py
```

Puis ouvrir l’URL Streamlit affichée, généralement `http://localhost:8501`.

## Exemple d’événement JSON

```json
{
  "timestamp": "2026-05-25T09:15:30Z",
  "user_id": "usr_9482",
  "user_city": "Paris",
  "product_id": "prod_5501",
  "product_cat": "Véhicules",
  "seller_id": "sel_0214",
  "action_type": "VOUT",
  "price": 450.0
}
```