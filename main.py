from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException
import sqlite3
from pydantic import BaseModel, Field, ValidationError

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
    # Connect to database
    con = sqlite3.connect(get_db_name())
    cur = con.cursor()
    
    # Build the date filter if days_ago is specified
    date_filter = ""
    if days_ago is not None:
        today = datetime.now().date()
        start_date = today - timedelta(days=days_ago)
        date_filter = f"AND t.date >= '{start_date}' AND t.date <= '{today}'"

    # Query to get spending by category for card transactions only
    query = f"""
        SELECT 
            m.category,
            SUM(t.amount_cents) as total_amount
        FROM transactions t
        JOIN merchants m ON t.merchant_id = m.id
        WHERE t.customer_id = ?
        AND t.is_card = 1
        {date_filter}
        GROUP BY m.category
        ORDER BY total_amount DESC
    """
    
    cur.execute(query, (f"customer-{customer_id}",))
    results = cur.fetchall()
    
    # Convert results to list of dictionaries
    categories = [
        {
            'category': category,
            'amount': amount
        }
        for category, amount in results
    ]

    # Limit to top N categories if specified
    if top_n:
        categories = categories[:top_n]
    
    con.close()
    return categories

class TransactionCreate(BaseModel):
    customer_id: int 
    merchant_id: str
    amount_cents: int = Field(gt=0, description="Amount in cents, must be positive")
    is_card: bool



@app.post("/transactions")
async def create_transaction(transaction: TransactionCreate):
    try:
        con = sqlite3.connect(get_db_name())
        cur = con.cursor()

        # Convert customer_id to correct format
        formatted_customer_id = f"customer-{transaction.customer_id}"

        # Verify merchant exists
        cur.execute("SELECT id FROM merchants WHERE id = ?", (transaction.merchant_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail=f"Merchant {transaction.merchant_id} not found")

        # Insert the transaction
        cur.execute("""
            INSERT INTO transactions 
            (customer_id, merchant_id, amount_cents, is_card, date) 
            VALUES (?, ?, ?, ?, ?)
        """, (
            formatted_customer_id,  # Use the formatted customer ID
            transaction.merchant_id,
            transaction.amount_cents,
            transaction.is_card,
            datetime.now().date()
        ))

        transaction_id = cur.lastrowid
        con.commit()
        con.close()

        return {
            "message": "Transaction created successfully",
            "transaction_id": transaction_id
        }
    
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        if 'con' in locals():
            con.close()




