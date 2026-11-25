import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel 
import httpx
from kafka import KafkaProducer
import json
import time

app = FastAPI(title="Order Service", description="A simple order service API", version="1.0.0")

CUSTOMER_SERVICE_URL = os.getenv("CUSTOMER_SERVICE_URL", "http://localhost:5000")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


class Order(BaseModel):
    order_id: int
    customer_id: int
    product: str

orders = {
    1: Order(order_id=1, customer_id=1, product="Laptop"),
    2: Order(order_id=2, customer_id=2, product="Keyboard")
}


@app.get("/orders/{order_id}")
async def get_order(order_id: int):
    order = orders.get(order_id)

    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    async with httpx.AsyncClient(timeout=3.0) as client:
        customer_response = await client.get(f"{CUSTOMER_SERVICE_URL}/customers/{order.customer_id}")

    if customer_response.status_code == 404:
        raise HTTPException(status_code=404, detail="Customer not found")
    
    customer = customer_response.json()

    return {
        "order_id": order.order_id,
        "product": order.product,
        "customer": customer
    }

@app.post("/orders")
def create_order():

    order = {
        "order_id": 101,
        "customer_id": 1,
        "product": "Laptop"
    }

    get_producer().send(
        "orders",
        {
            "event": "OrderCreated",
            **order
        }
    )

    return order

# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

producer = None

def get_producer():
    global producer
    if producer is None:
        for _ in range(10):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                )
                break
            except Exception:
                time.sleep(3)
        else:
            raise RuntimeError("Kafka indisponible")
    return producer
