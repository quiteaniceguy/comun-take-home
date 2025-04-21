import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Optional

def get_db_name():
    return "my_db.db"

def create_transaction_in_db(customer_id: str, merchant_id: str, amount_cents: int, is_card: bool) -> int | None:
    """Create a new transaction and return its ID"""
    try:
        con = sqlite3.connect(get_db_name())
        cur = con.cursor()

        # Verify merchant exists
        cur.execute("SELECT id FROM merchants WHERE id = ?", (merchant_id,))
        if not cur.fetchone():
            return None  # Merchant not found

        # Insert the transaction
        cur.execute("""
            INSERT INTO transactions 
            (customer_id, merchant_id, amount_cents, is_card, date) 
            VALUES (?, ?, ?, ?, ?)
        """, (
            customer_id,
            merchant_id,
            amount_cents,
            is_card,
            datetime.now().date()
        ))

        transaction_id = cur.lastrowid
        con.commit()
        return transaction_id
    finally:
        con.close()

def get_customer_insights(customer_id: str, top_n: Optional[int] = None, days_ago: Optional[int] = None) -> List[Dict]:
    """Get spending insights for a customer"""
    try:
        con = sqlite3.connect(get_db_name())
        cur = con.cursor()
        
        date_filter = ""
        if days_ago is not None:
            today = datetime.now().date()
            start_date = today - timedelta(days=days_ago)
            # Include transactions from start_date AND today (inclusive range)
            date_filter = f"AND DATE(t.date) >= DATE('{start_date}') AND DATE(t.date) <= DATE('{today}')"

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
        
        # Convert to list of dicts
        insights = [
            {"category": category, "amount": amount}
            for category, amount in results
        ]
        
        # Apply top_n filter if specified
        if top_n is not None:
            insights = insights[:top_n]
            
        return insights
    finally:
        con.close()

def verify_merchant_exists(merchant_id: str) -> bool:
    """Check if a merchant exists in the database"""
    try:
        con = sqlite3.connect(get_db_name())
        cur = con.cursor()
        cur.execute("SELECT id FROM merchants WHERE id = ?", (merchant_id,))
        return cur.fetchone() is not None
    finally:
        con.close() 