import json
import logging
import os
import time
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from pydantic import BaseModel, Field
from pymongo import MongoClient, ReturnDocument

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("order-service")

CUSTOMER_SERVICE_URL = os.getenv("CUSTOMER_SERVICE_URL", "http://localhost:5000")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

db = MongoClient(MONGO_URI)["orders_db"]
orders_col = db["orders"]
counters_col = db["counters"]

app = FastAPI(
    title="Order Service",
    description="Création et consultation des commandes",
    version="2.0.0",
)


class OrderIn(BaseModel):
    customer_id: int
    product: str = Field(..., min_length=1)
    quantity: int = Field(1, ge=1)
    price: float = Field(0.0, ge=0)


# ---------- Kafka ----------
producer = None


def get_producer() -> KafkaProducer:
    global producer
    if producer is None:
        for _ in range(5):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                )
                break
            except Exception as e:
                log.warning("Kafka indisponible, nouvelle tentative : %s", e)
                time.sleep(2)
        else:
            raise RuntimeError("Kafka indisponible")
    return producer


def publish_event(event: dict) -> bool:
    try:
        get_producer().send("orders", event).get(timeout=10)
        return True
    except Exception as e:
        log.error("Échec de publication de l'événement : %s", e)
        return False


# ---------- Utilitaires ----------
def next_order_id() -> int:
    doc = counters_col.find_one_and_update(
        {"_id": "order_id"},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return doc["seq"]


def fetch_customer(customer_id: int) -> dict:
    try:
        r = httpx.get(f"{CUSTOMER_SERVICE_URL}/customers/{customer_id}", timeout=3.0)
    except httpx.RequestError:
        raise HTTPException(status_code=503, detail="Customer service unavailable")
    if r.status_code == 404:
        raise HTTPException(status_code=404, detail="Customer not found")
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail="Unexpected customer service error")
    return r.json()


# ---------- Routes ----------
@app.get("/")
def home():
    return {"service": "Order Service", "status": "running"}


@app.post("/orders", status_code=201)
def create_order(data: OrderIn):
    # 1. Le client doit exister
    fetch_customer(data.customer_id)

    # 2. Enregistrement de la commande
    order = {
        "order_id": next_order_id(),
        **data.model_dump(),
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    orders_col.insert_one(order.copy())

    # 3. Publication de l'événement pour les autres services
    published = publish_event({"event": "OrderCreated", **order})

    return {**order, "event_published": published}


@app.get("/orders")
def list_orders():
    return list(orders_col.find({}, {"_id": 0}))


@app.get("/orders/{order_id}")
def get_order(order_id: int):
    order = orders_col.find_one({"order_id": order_id}, {"_id": 0})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    customer = fetch_customer(order["customer_id"])
    return {**order, "customer": customer}