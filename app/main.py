from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException
import sqlite3
from pydantic import BaseModel, Field, ValidationError

from app.db_repository import create_transaction_in_db, get_customer_insights

app = FastAPI()

def get_db_name():
    return "my_db.db"

class InsightsInput(BaseModel):
    customer_id: int = Field(description="Customer ID")
    top_n: int | None = Field(default=None, ge=0, description="Number of top categories to return")
    days_ago: int | None = Field(default=None, ge=0, description="Number of days to look back")

@app.get("/insights")
async def get_insights(customer_id: str, top_n: int | None = None, days_ago: int | None = None) -> list[dict]:

    try:
        InsightsInput(customer_id=customer_id, top_n=top_n, days_ago=days_ago)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    return await _get_insights(customer_id, top_n, days_ago)


async def _get_insights(customer_id: str, top_n: int | None = None, days_ago: int | None = None) -> list[dict]:
    insights = get_customer_insights(customer_id, top_n, days_ago)
    return insights 

class TransactionCreate(BaseModel):
    customer_id: int 
    merchant_id: str
    amount_cents: int = Field(gt=0, description="Amount in cents, must be positive")
    is_card: bool


@app.post("/transactions")
async def create_transaction(transaction: TransactionCreate) -> dict:
    try:
        return await _create_transaction(transaction)
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
 


async def _create_transaction(transaction: TransactionCreate) -> dict:
    # Format customer_id
    customer_id = f"customer-{transaction.customer_id}"
    
    # Create transaction
    transaction_id = create_transaction_in_db(
        customer_id=customer_id,
        merchant_id=transaction.merchant_id,
        amount_cents=transaction.amount_cents,
        is_card=transaction.is_card
    )
    
    if transaction_id is None:
        raise HTTPException(status_code=404, detail=f"Merchant {transaction.merchant_id} not found")
        
    return {
        "message": "Transaction created successfully",
        "transaction_id": transaction_id
    }


