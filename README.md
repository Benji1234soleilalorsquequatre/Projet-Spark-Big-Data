# Projet d’Ingénierie Big Data & Analyse de Graphes Temps Réel

Plateforme de streaming d’interactions commerciales inspirée de LeBonCoin.  
Le projet simule des événements utilisateur en JSON, les traite avec PySpark Structured Streaming, construit un graphe dynamique à chaque micro-batch et visualise les résultats dans un dashboard Streamlit.

## Architecture

```text
Simulateur Python JSON
        ↓ fichiers JSON générés en continu
Dossier data/input_json surveillé par Spark
        ↓ readStream + schéma strict
PySpark Structured Streaming
        ↓ watermark + fenêtres glissantes
Agrégations temporelles + KPIs simples
        ↓ foreachBatch
Construction du graphe dynamique
        ↓ Parquet dans output/
Dashboard Streamlit rafraîchi toutes les 5 s
```

## Structure

```text
Projet-Spark-Big-Data/
├── README.md
├── requirements.txt
├── .gitignore
├── data/
│   └── input_json/              # fichiers JSON générés en continu
├── src/
│   ├── producer.py              # générateur de données
│   ├── spark_streaming.py        # pipeline Structured Streaming
│   ├── graph_builder.py          # construction des vertices/edges + KPIs
│   └── dashboard.py              # dashboard Streamlit
├── output/                       # sorties Spark lues par le dashboard
└── checkpoints/                  # checkpoints Spark Streaming
```

## Prérequis

- Python 3.10 ou supérieur
- Java compatible avec Apache Spark
- Apache Spark 3.5.x
- Hadoop / winutils configuré sous Windows
- Un environnement virtuel Python avec les dépendances du fichier `requirements.txt`

## Installation locale

Créer l’environnement virtuel :

```powershell
python -m venv .venv
```

Activer l’environnement virtuel :

```powershell
.\.venv\Scripts\Activate.ps1
```

Installer les dépendances :

```powershell
python -m pip install -r requirements.txt
```

## Lancement du projet

Le projet se lance avec trois terminaux PowerShell séparés.

Avant de lancer les commandes, se placer dans le dossier du projet :

```powershell
cd "CHEMIN\VERS\Projet-Spark-Big-Data"
```

Remplacer `CHEMIN\VERS\Projet-Spark-Big-Data` par le chemin réel du projet sur la machine.

---

## Terminal 1 — Générateur d’événements

Activer l’environnement virtuel :

```powershell
.\.venv\Scripts\Activate.ps1
```

Lancer le producteur :

```powershell
python .\src\producer.py --output-dir .\data\input_json --batch-size 50 --sleep 2
```

Ce terminal génère en continu des fichiers JSON dans le dossier `data/input_json`.

---

## Terminal 2 — Spark Structured Streaming

Activer l’environnement virtuel :

```powershell
.\.venv\Scripts\Activate.ps1
```

Définir les variables d’environnement nécessaires à Spark sous Windows :

```powershell
$env:SPARK_HOME="C:\spark\spark-3.5.8-bin-hadoop3"
$env:HADOOP_HOME="C:\hadoop"
${env:hadoop.home.dir}="C:\hadoop"
$env:PYSPARK_PYTHON="$PWD\.venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON="$PWD\.venv\Scripts\python.exe"
$env:SPARK_LOCAL_IP="127.0.0.1"
$env:PATH="$env:HADOOP_HOME\bin;$env:SPARK_HOME\bin;$env:PATH"
```

Lancer Spark Structured Streaming avec cette commande PowerShell sur une seule ligne :

```powershell
spark-submit --conf "spark.driver.host=127.0.0.1" --conf "spark.driver.bindAddress=127.0.0.1" --conf "spark.ui.host=127.0.0.1" .\src\spark_streaming.py --input-dir .\data\input_json --output-dir .\output --checkpoint-dir .\checkpoints --shuffle-partitions 4 --driver-memory 2g
```

Important : en PowerShell, il ne faut pas utiliser `\` pour couper une commande sur plusieurs lignes.  
Le caractère `\` est utilisé dans les terminaux Linux/bash, mais pas dans PowerShell.

Si une commande multi-lignes est souhaitée dans PowerShell, il faut utiliser le caractère backtick :

```powershell
spark-submit `
  --conf "spark.driver.host=127.0.0.1" `
  --conf "spark.driver.bindAddress=127.0.0.1" `
  --conf "spark.ui.host=127.0.0.1" `
  .\src\spark_streaming.py `
  --input-dir .\data\input_json `
  --output-dir .\output `
  --checkpoint-dir .\checkpoints `
  --shuffle-partitions 4 `
  --driver-memory 2g
```

---

## Terminal 3 — Dashboard Streamlit

Activer l’environnement virtuel :

```powershell
.\.venv\Scripts\Activate.ps1
```

Lancer le dashboard :

```powershell
python -m streamlit run .\src\dashboard.py
```

Puis ouvrir l’URL Streamlit affichée, généralement :

```text
http://localhost:8501
```

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

## Graphe dynamique

Le graphe est construit à partir des événements traités par Spark.

Les sommets du graphe sont :

```text
USER
SELLER
PRODUCT
```

Les arêtes du graphe sont :

```text
USER   → PRODUCT : AIME
USER   → PRODUCT : VOUT
USER   → PRODUCT : ACHAT
SELLER → PRODUCT : PROPOSE
```

La relation `PROPOSE` représente le lien entre un vendeur et les produits qu’il propose.

## KPIs affichés

Le dashboard affiche des indicateurs globaux simples :

- nombre total d’événements ;
- nombre total d’achats ;
- chiffre d’affaires ;
- nombre d’utilisateurs uniques ;
- nombre de produits uniques ;
- nombre de vendeurs uniques.

## Fenêtres temporelles

Spark regroupe les événements dans des fenêtres temporelles glissantes afin d’analyser l’évolution du flux dans le temps.

Les fenêtres permettent d’afficher, pour chaque intervalle :

- le début de la fenêtre ;
- la fin de la fenêtre ;
- le nombre total d’événements ;
- le nombre de `AIME` ;
- le nombre de `VOUT` ;
- le nombre de `ACHAT` ;
- le chiffre d’affaires de la fenêtre.

Cette partie permet de suivre l’activité commerciale presque en temps réel.

## Nettoyage des sorties

Pour repartir sur une exécution propre :

```powershell
Remove-Item -Recurse -Force .\output -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .\checkpoints -ErrorAction SilentlyContinue

New-Item -ItemType Directory -Force .\output
New-Item -ItemType Directory -Force .\checkpoints
New-Item -ItemType Directory -Force .\data\input_json
```

## Fichiers générés

Les dossiers suivants contiennent des fichiers générés automatiquement :

```text
data/input_json/
output/
checkpoints/
```
