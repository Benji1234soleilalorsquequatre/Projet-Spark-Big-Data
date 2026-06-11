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

## Choix technique principal

Le flux est basé sur un dossier de fichiers JSON plutôt que Kafka. C’est le choix le plus robuste pour un projet étudiant local : aucune dépendance externe lourde, comportement reproductible, intégration native avec Spark Structured Streaming, démonstration claire du streaming infini.

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
├── report/rapport_technique.md
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

## Concepts Spark couverts

- `SparkSession` comme point d’entrée applicatif.
- `SparkContext` récupéré depuis la session pour le logging et la configuration runtime.
- Configuration mémoire : `spark.driver.memory`, `spark.executor.memory`.
- Configuration des shuffles : `spark.sql.shuffle.partitions`.
- Structured Streaming avec DataFrames de streaming.
- Schéma strict `StructType`, sans inférence automatique.
- Conversion du champ `timestamp` en `timestamp_ts`.
- `withWatermark("timestamp_ts", "2 minutes")` pour gérer les retards.
- Fenêtres glissantes de 1 minute avec slide de 30 secondes.
- Mode `update` pour les agrégations temporelles, car une fenêtre peut évoluer avant clôture par watermark.
- Mode `append` pour le flux envoyé à `foreachBatch`.
- GraphFrames sur micro-batches statiques via `foreachBatch()`.

## Sorties générées

- `output/windows/` : agrégations temporelles en Parquet.
- `output/graph_vertices/` : sommets `id`, `type`, `label`.
- `output/graph_edges/` : arêtes `src`, `dst`, `relationship`.
- `output/graph_metrics/` : degrés et centralité approximée.
- `output/kpis/` : KPIs du dernier micro-batch.
- `output/top_products/`, `output/top_sellers/`, `output/top_cities/`.

## Captures attendues dans le rapport

1. Terminal du producteur montrant l’écriture continue des fichiers.
2. Spark UI ou logs Structured Streaming montrant les micro-batches.
3. Dashboard avec graphe dynamique.
4. Table des fenêtres temporelles.
5. Top produits, vendeurs et villes.

## Difficultés techniques

GraphFrames n’est pas un opérateur streaming natif. La solution correcte est d’utiliser `foreachBatch()` : Spark matérialise chaque micro-batch en DataFrame statique, puis GraphFrames peut être appliqué. Si GraphFrames n’est pas disponible localement, le projet reste démontrable grâce aux métriques de degré calculées en DataFrames Spark.

## Améliorations possibles

- Remplacer le dossier JSON par Kafka.
- Persister les résultats dans Delta Lake.
- Ajouter une base Neo4j pour l’historique du graphe.
- Déployer Spark en cluster Docker ou Kubernetes.
- Ajouter des tests unitaires et des tests de charge.
- Améliorer le layout du graphe pour très gros volumes.
