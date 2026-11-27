import json
import logging
import os
import socket
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from kafka import KafkaConsumer
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("analytics-service")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

health_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)  # sondes : échec rapide
db = MongoClient(MONGO_URI)["analytics_db"]
global_col = db["global"]
products_col = db["products"]
hourly_col = db["hourly"]
customers_col = db["customers"]


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

    customer_id = event.get("customer_id")
    if customer_id is not None:
        customers_col.update_one(
            {"_id": customer_id},
            {
                "$inc": {"orders": 1, "revenue": amount},
                "$max": {"last_order_at": created_at},
                "$min": {"first_order_at": created_at},
            },
            upsert=True,
        )

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


consumer_thread = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global consumer_thread
    consumer_thread = threading.Thread(target=consume, daemon=True)
    consumer_thread.start()
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


# ---------- Health checks ----------
def mongo_status() -> str:
    try:
        health_client.admin.command("ping")
        return "ok"
    except Exception as e:
        return f"error: {type(e).__name__}"

def kafka_status() -> str:
    """Test TCP simple vers le premier broker (indépendant de la version de la bibliothèque)."""
    host, _, port = KAFKA_BOOTSTRAP_SERVERS.split(",")[0].partition(":")
    try:
        with socket.create_connection((host, int(port or 9092)), timeout=2):
            return "ok"
    except OSError as e:
        return f"error: {type(e).__name__}"


def consumer_status() -> str:
    return "ok" if consumer_thread is not None and consumer_thread.is_alive() else "error: consumer not running"

@app.get("/health")
def health():
    """Liveness : le processus répond."""
    return {"status": "ok"}


@app.get("/health/ready")
def ready():
    """Readiness : les dépendances du service sont joignables."""
    checks = {"mongo": mongo_status(), "kafka": kafka_status(), "consumer": consumer_status()}
    ok = all(v == "ok" for v in checks.values())
    return JSONResponse(
        status_code=200 if ok else 503,
        content={"status": "ready" if ok else "not_ready", "checks": checks},
    )


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


@app.get("/stats/customers")
def stats_customers(limit: int = Query(500, ge=1, le=5000)):
    """Agrégats par client (identifiants numériques uniquement), utilisés pour la segmentation RFM."""
    docs = customers_col.find().sort("revenue", -1).limit(limit)
    return [
        {
            "customer_id": d["_id"],
            "orders": d.get("orders", 0),
            "revenue": round(d.get("revenue", 0), 2),
            "first_order_at": d.get("first_order_at"),
            "last_order_at": d.get("last_order_at"),
        }
        for d in docs
    ]


@app.get("/stats/hourly")
def stats_hourly(hours: int = Query(24, ge=1, le=720)):
    """Commandes par heure (les `hours` dernières heures qui comptent au moins une commande)."""
    docs = hourly_col.find().sort("_id", -1).limit(hours)
    return [{"hour": d["_id"], "orders": d.get("orders", 0)} for d in docs][::-1]
