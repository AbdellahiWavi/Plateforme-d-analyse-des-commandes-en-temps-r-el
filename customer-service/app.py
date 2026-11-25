import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

db = MongoClient(MONGO_URI)["customers_db"]
customers_col = db["customers"]
counters_col = db["counters"]


class CustomerIn(BaseModel):
    name: str = Field(..., min_length=1)
    email: str = Field(..., pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class Customer(CustomerIn):
    id: int


def next_id() -> int:
    """Identifiant auto-incrémenté, atomique côté MongoDB."""
    doc = counters_col.find_one_and_update(
        {"_id": "customer_id"},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return doc["seq"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    customers_col.create_index("email", unique=True)
    # Données de démarrage si la base est vide
    if customers_col.count_documents({}) == 0:
        customers_col.insert_many([
            {"id": 1, "name": "Ahmed", "email": "ahmed@gmail.com"},
            {"id": 2, "name": "Salah", "email": "salah@gmail.com"},
        ])
        counters_col.update_one({"_id": "customer_id"}, {"$set": {"seq": 2}}, upsert=True)
    yield


app = FastAPI(
    title="Customer Service",
    description="Gestion et enregistrement des clients",
    version="2.0.0",
    lifespan=lifespan,
)


@app.get("/")
def home():
    return {"service": "Customer Service", "status": "running"}


@app.post("/customers", response_model=Customer, status_code=201)
def create_customer(data: CustomerIn):
    customer = {"id": next_id(), **data.model_dump()}
    try:
        customers_col.insert_one(customer.copy())  # copy : Mongo ajoute _id au dict
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="Email already registered")
    return customer


@app.get("/customers", response_model=list[Customer])
def list_customers():
    return list(customers_col.find({}, {"_id": 0}))


@app.get("/customers/{id}", response_model=Customer)
def get_customer(id: int):
    customer = customers_col.find_one({"id": id}, {"_id": 0})
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    return customer