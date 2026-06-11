from __future__ import annotations

import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

CITIES = [
    "Paris", "Lyon", "Marseille", "Toulouse", "Nice", "Nantes", "Bordeaux",
    "Lille", "Strasbourg", "Rennes", "Grenoble", "Montpellier"
]
CATEGORIES = [
    "Véhicules", "Immobilier", "Multimédia", "Maison", "Mode", "Loisirs",
    "Bricolage", "Electroménager", "Sports", "Jardin"
]
ACTION_WEIGHTS = {
    "AIME": 0.65,
    "VOUT": 0.25,
    "ACHAT": 0.10,
}
PRICE_BY_CATEGORY = {
    "Véhicules": (800, 25000),
    "Immobilier": (400, 250000),
    "Multimédia": (20, 2000),
    "Maison": (10, 1500),
    "Mode": (5, 300),
    "Loisirs": (10, 800),
    "Bricolage": (15, 1000),
    "Electroménager": (30, 3000),
    "Sports": (10, 1200),
    "Jardin": (10, 1500),
}


def weighted_action() -> str:
    actions = list(ACTION_WEIGHTS.keys())
    weights = list(ACTION_WEIGHTS.values())
    return random.choices(actions, weights=weights, k=1)[0]


def generate_catalog(nb_products: int, nb_sellers: int) -> List[Dict[str, str]]:
    catalog = []
    for i in range(1, nb_products + 1):
        category = random.choice(CATEGORIES)
        seller_id = f"sel_{random.randint(1, nb_sellers):04d}"
        catalog.append({
            "product_id": f"prod_{i:05d}",
            "product_cat": category,
            "seller_id": seller_id,
        })
    return catalog


def make_event(catalog: List[Dict[str, str]], nb_users: int, late_event_probability: float) -> Dict[str, object]:
    product = random.choice(catalog)
    min_price, max_price = PRICE_BY_CATEGORY[product["product_cat"]]

    now = datetime.now(timezone.utc)
    # Introduit volontairement quelques événements en retard pour démontrer withWatermark().
    if random.random() < late_event_probability:
        now = now - timedelta(seconds=random.randint(20, 90))

    return {
        "timestamp": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "user_id": f"usr_{random.randint(1, nb_users):04d}",
        "user_city": random.choice(CITIES),
        "product_id": product["product_id"],
        "product_cat": product["product_cat"],
        "seller_id": product["seller_id"],
        "action_type": weighted_action(),
        "price": round(random.uniform(min_price, max_price), 2),
    }


def write_batch(output_dir: Path, events: List[Dict[str, object]]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"events_{int(time.time() * 1000)}_{random.randint(1000, 9999)}.json"
    tmp_path = output_dir / f".{file_name}.tmp"
    final_path = output_dir / file_name

    with tmp_path.open("w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

    # Rename atomique : évite que Spark lise un fichier en cours d'écriture.
    tmp_path.rename(final_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Streaming infini JSON pour projet Spark GraphFrames")
    parser.add_argument("--output-dir", default="data/input_json", help="Dossier surveillé par Spark")
    parser.add_argument("--batch-size", type=int, default=50, help="Nombre d'événements par fichier")
    parser.add_argument("--sleep", type=float, default=2.0, help="Pause entre deux fichiers")
    parser.add_argument("--users", type=int, default=1000)
    parser.add_argument("--sellers", type=int, default=150)
    parser.add_argument("--products", type=int, default=2000)
    parser.add_argument("--late-probability", type=float, default=0.05)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    catalog = generate_catalog(args.products, args.sellers)

    print(f"Production infinie vers {output_dir.resolve()} - CTRL+C pour arrêter.")
    try:
        while True:
            events = [make_event(catalog, args.users, args.late_probability) for _ in range(args.batch_size)]
            write_batch(output_dir, events)
            print(f"+ {len(events)} événements écrits")
            time.sleep(args.sleep)
    except KeyboardInterrupt:
        print("Arrêt du producteur.")


if __name__ == "__main__":
    main()
