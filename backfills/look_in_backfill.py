import sqlite3

def get_user_transactions(customer_id):
    # Connect to database
    con = sqlite3.connect("my_db.db")
    cur = con.cursor()
    
    # Query transactions joined with merchant info
    cur.execute("""
        SELECT 
            t.id,
            t.amount_cents,
            t.is_card,
            t.date,
            m.name as merchant_name,
            m.category as merchant_category
        FROM transactions t
        JOIN merchants m ON t.merchant_id = m.id
        WHERE t.customer_id = ?
        ORDER BY t.date
    """, (f"customer-{customer_id}",))
    
    transactions = cur.fetchall()
    
    # Print transactions in a readable format
    print(f"\nTransactions for customer-{customer_id}:")
    print("-" * 80)
    for t in transactions:
        id, amount, is_card, date, merchant, category = t
        amount_dollars = amount / 100  # Convert cents to dollars
        payment_method = "card" if is_card else "cash"
        print(f"ID: {id}")
        print(f"Amount: ${amount_dollars:.2f}")
        print(f"Date: {date}")
        print(f"Merchant: {merchant} ({category})")
        print(f"Payment Method: {payment_method}")
        print("-" * 80)
    
    # Print summary
    total_spent = sum(t[1] for t in transactions) / 100
    print(f"\nTotal transactions: {len(transactions)}")
    print(f"Total spent: ${total_spent:.2f}")
    
    con.close()

if __name__ == "__main__":
    get_user_transactions(1)
