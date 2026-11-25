import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI
from kafka import KafkaConsumer
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("analytics-service")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

db = MongoClient(MONGO_URI)["analytics_db"]
global_col = db["global"]
products_col = db["products"]
hourly_col = db["hourly"]


def process(event: dict) -> None:
    if event.get("event") != "OrderCreated":
        return

    quantity = event.get("quantity", 1)
    amount = event.get("price", 0) * quantity
    product = event.get("product", "unknown")

    # created_at = "2026-11-25T14:03:11+00:00" -> bucket "2026-11-25T14"
    created_at = event.get("created_at") or datetime.utcnow().isoformat()
    hour = created_at[:13]

    global_col.update_one(
        {"_id": "global"},
        {"$inc": {"total_orders": 1, "revenue": amount}},
        upsert=True,
    )
    products_col.update_one(
        {"_id": product},
        {"$inc": {"orders": 1, "quantity": quantity, "revenue": amount}},
        upsert=True,
    )
    hourly_col.update_one({"_id": hour}, {"$inc": {"orders": 1}}, upsert=True)

    log.info("Order received : %s (product=%s, amount=%.2f)", event.get("order_id"), product, amount)


def consume() -> None:
    """Boucle de consommation Kafka, relancée en cas d'erreur."""
    while True:
        try:
            consumer = KafkaConsumer(
                "orders",
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id="analytics-group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            log.info("Analytics consumer started")
            for message in consumer:
                process(message.value)
        except Exception as e:
            log.error("Consumer error, retry in 5s : %s", e)
            time.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    threading.Thread(target=consume, daemon=True).start()
    yield


app = FastAPI(
    title="Analytics Service",
    description="Statistiques de commandes calculées en temps réel depuis Kafka",
    version="2.0.0",
    lifespan=lifespan,
)


@app.get("/")
def home():
    return {"service": "Analytics Service", "status": "running"}


@app.get("/stats")
def stats():
    g = global_col.find_one({"_id": "global"}) or {}
    total = g.get("total_orders", 0)
    revenue = round(g.get("revenue", 0), 2)
    return {
        "total_orders": total,
        "revenue": revenue,
        "average_order_value": round(revenue / total, 2) if total else 0,
    }


@app.get("/stats/products")
def stats_products():
    docs = products_col.find().sort("orders", -1)
    return [
        {"product": d["_id"], "orders": d.get("orders", 0),
         "quantity": d.get("quantity", 0), "revenue": round(d.get("revenue", 0), 2)}
        for d in docs
    ]


@app.get("/stats/hourly")
def stats_hourly():
    docs = hourly_col.find().sort("_id", -1).limit(24)
    return [{"hour": d["_id"], "orders": d.get("orders", 0)} for d in docs][::-1]