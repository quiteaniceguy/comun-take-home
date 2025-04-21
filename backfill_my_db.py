import sqlite3
import csv

def backfill_merchants():
    # Connect to database
    con = sqlite3.connect("my_db.db")
    cur = con.cursor()

    # Create merchants table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS merchants(
            id TEXT PRIMARY KEY,
            name TEXT,
            category TEXT
        )
    """)

    # Read CSV and insert data
    with open('merchants_2024_12_05.csv', 'r') as file:
        # Skip header row
        csv_reader = csv.reader(file)
        next(csv_reader)
        
        # Insert each merchant
        for row in csv_reader:
            merchant_id, name, category = row
            cur.execute(
                "INSERT INTO merchants (id, name, category) VALUES (?, ?, ?)",
                (merchant_id, name, category)
            )

    # Create transactions table with auto-incrementing id
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id TEXT,
            merchant_id TEXT,
            amount_cents INTEGER,
            is_card BOOLEAN,
            date DATE,
            FOREIGN KEY(merchant_id) REFERENCES merchants(id)
        )
    """)

    # Read transactions CSV and insert data
    with open('transactions_2024_12_05.csv', 'r') as file:
        # Skip header row
        csv_reader = csv.reader(file)
        next(csv_reader)
        
        # Insert each transaction
        for row in csv_reader:
            _, customer_id, merchant_id, amount_cents, is_card, date = row  # Ignore the transaction-X id from CSV
            # Convert is_card string to boolean
            is_card = is_card.lower() == 'true'
            
            cur.execute("""
                INSERT INTO transactions 
                (customer_id, merchant_id, amount_cents, is_card, date) 
                VALUES (?, ?, ?, ?, ?)
                """,
                (customer_id, merchant_id, int(amount_cents), is_card, date)
            )

    # Commit and close
    con.commit()
    con.close()

if __name__ == "__main__":
    backfill_merchants()
