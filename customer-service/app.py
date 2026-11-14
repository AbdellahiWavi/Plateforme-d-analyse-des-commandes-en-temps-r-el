from fastapi import FastAPI, HTTPException
from pydantic import BaseModel 

app = FastAPI(title="Customer Service", description="A simple customer service API", version="1.0.0")

class Customer(BaseModel):
    id: int
    name: str
    email: str

customers = {
    1: Customer(id=1, name="Ahmed", email="ahmed@gmail.com"),
    2: Customer(id=2, name="Salah", email="salah@gmail.com")
}

@app.get("/")
def home():
    return {
        "service": "Customer Service",
        "status": "running"
    }

@app.get("/customers/{id}")
async def get_customer(id: int):
    customer = customers.get(id)

    if customer:
        return customer

    raise HTTPException(status_code=404, detail="Customer not found")