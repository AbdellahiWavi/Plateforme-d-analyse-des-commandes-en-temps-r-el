import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel 
import httpx

app = FastAPI(title="Order Service", description="A simple order service API", version="1.0.0")

CUSTOMER_SERVICE_URL = os.getenv("CUSTOMER_SERVICE_URL", "http://localhost:5000")


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
