"""Injecte un historique de commandes simulées dans Kafka (topic « orders »).

Utile pour démontrer les analyses (tendance, pic, anomalie, prévision, segments RFM),
qui ont besoin de plusieurs heures d'historique, et pour tester une charge importante.

Prérequis : Docker Compose lancé (listener externe de Kafka sur localhost:9092) et
            pip install kafka-python
Exemples  : python scripts/seed_history.py
            python scripts/seed_history.py --hours 72 --per-hour 500   # charge plus forte

Attention : ces événements arrivent dans analytics-service, mais ne sont pas enregistrés
dans la base d'order-service (c'est une simulation).
"""
import argparse
import json
import random
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

PRODUCTS = [("Laptop", 900.0), ("Keyboard", 45.0), ("Mouse", 25.0), ("Monitor", 220.0), ("Headset", 70.0)]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--hours", type=int, default=48, help="nombre d'heures d'historique")
    ap.add_argument("--per-hour", type=int, default=6, help="commandes moyennes par heure")
    ap.add_argument("--customers", type=int, default=5, help="identifiants clients de 1 à N")
    ap.add_argument("--spike-hours-ago", type=int, default=10, help="heure du pic simulé (0 = aucun)")
    ap.add_argument("--spike-factor", type=int, default=8, help="multiplicateur du pic")
    args = ap.parse_args()

    rng = random.Random(42)  # reproductible
    now_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    order_id = 100000  # plage dédiée, distincte des vraies commandes
    sent = 0
    for hours_ago in range(args.hours, 0, -1):  # heures terminées uniquement
        base = now_hour - timedelta(hours=hours_ago)
        n = max(0, int(rng.gauss(args.per_hour, max(1.0, args.per_hour * 0.25))))
        if args.spike_hours_ago and hours_ago == args.spike_hours_ago:
            n *= args.spike_factor
        for _ in range(n):
            product, price = rng.choice(PRODUCTS)
            created = base + timedelta(minutes=rng.randint(0, 59), seconds=rng.randint(0, 59))
            order_id += 1
            producer.send("orders", {
                "event": "OrderCreated",
                "order_id": order_id,
                "customer_id": rng.randint(1, args.customers),
                "product": product,
                "quantity": rng.randint(1, 3),
                "price": price,
                "created_at": created.isoformat(timespec="seconds"),
            })
            sent += 1

    producer.flush()
    print(f"{sent} événements envoyés sur {args.hours} heures (pic simulé il y a {args.spike_hours_ago} h).")


if __name__ == "__main__":
    main()
