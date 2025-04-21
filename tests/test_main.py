from datetime import datetime, timedelta
from freezegun import freeze_time
import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient
import sqlite3
from app import app
import os

TEST_DB = "test.db"

@pytest.fixture
def test_db():
    """Create a fresh test database and clean it up after"""
    # Remove test db if it exists
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    
    # Create and populate test database
    con = sqlite3.connect(TEST_DB)
    cur = con.cursor()
    
    # Create tables
    cur.execute("""
        CREATE TABLE merchants(
            id TEXT PRIMARY KEY,
            name TEXT,
            category TEXT
        )
    """)
    
    cur.execute("""
        CREATE TABLE transactions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id TEXT,
            merchant_id TEXT,
            amount_cents INTEGER,
            is_card BOOLEAN,
            date DATE,
            FOREIGN KEY(merchant_id) REFERENCES merchants(id)
        )
    """)
    
    # Insert test merchants
    test_merchants = [
        ("merchant-1", "Test Store", "food"),
        ("merchant-2", "Game Shop", "gaming"),
        ("merchant-3", "Power Co", "utilities")
    ]
    cur.executemany("INSERT INTO merchants (id, name, category) VALUES (?, ?, ?)", test_merchants)
    
    con.commit()
    con.close()
    
    yield TEST_DB
    
    # Cleanup after tests
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

@pytest.fixture
def client(test_db):
    with patch('app.db_repository.get_db_name', return_value=TEST_DB):
        yield TestClient(app)

def test_create_and_get_insights(client, test_db):
    """Test creating a transaction and getting insights"""
    # Create a transaction
    transaction = {
        "customer_id": 1,
        "merchant_id": "merchant-1",
        "amount_cents": 5000,
        "is_card": True
    }
    response = client.post("/transactions", json=transaction)
    assert response.status_code == 200
    
    # Get insights
    response = client.get("/insights?customer_id=1")
    assert response.status_code == 200
    insights = response.json()
    
    # Verify the insights
    assert len(insights) > 0
    assert insights[0]["category"] == "food"
    assert insights[0]["amount"] == 5000

def test_invalid_merchant(client, test_db):
    """Test creating a transaction with invalid merchant"""
    transaction = {
        "customer_id": 1,
        "merchant_id": "merchant-999",  # Non-existent merchant
        "amount_cents": 5000,
        "is_card": True
    }
    response = client.post("/transactions", json=transaction)
    assert response.status_code == 404


def test_insights_top_n_parameter(client, test_db):
    """Test that top_n parameter correctly limits categories returned"""
    # Create transactions in different categories
    transactions = [
        {
            "customer_id": 1,
            "merchant_id": "merchant-1",  # food
            "amount_cents": 5000,
            "is_card": True
        },
        {
            "customer_id": 1,
            "merchant_id": "merchant-2",  # gaming
            "amount_cents": 3000,
            "is_card": True
        },
        {
            "customer_id": 1,
            "merchant_id": "merchant-3",  # utilities
            "amount_cents": 1000,
            "is_card": True
        }
    ]
    
    for t in transactions:
        response = client.post("/transactions", json=t)
        assert response.status_code == 200
    
    # Test top_n=2 returns only top 2 categories by spend
    response = client.get("/insights?customer_id=1&top_n=2")
    assert response.status_code == 200
    insights = response.json()
    
    assert len(insights) == 2
    assert insights[0]["category"] == "food"
    assert insights[0]["amount"] == 5000
    assert insights[1]["category"] == "gaming"
    assert insights[1]["amount"] == 3000

def test_insights_days_ago_parameter(client, test_db):
    """Test that days_ago parameter correctly filters transactions by date"""
    today = datetime(2024, 1, 10)  # Fix a specific date for testing
    
    with freeze_time(today):
        # Create transactions
        transactions = [
            {
                "customer_id": 1,
                "merchant_id": "merchant-1",
                "amount_cents": 5000,
                "is_card": True
            },
            {
                "customer_id": 1,
                "merchant_id": "merchant-1",
                "amount_cents": 3000,
                "is_card": True
            }
        ]
        
        # Create first transaction at current time
        response = client.post("/transactions", json=transactions[0])
        assert response.status_code == 200

        # Move time back 5 days and create second transaction
        with freeze_time(today - timedelta(days=5)):
            response = client.post("/transactions", json=transactions[1])
            assert response.status_code == 200
        
        # Test last 2 days (should only include first transaction)
        response = client.get("/insights?customer_id=1&days_ago=2")
        assert response.status_code == 200
        insights = response.json()
        
        assert len(insights) == 1
        assert insights[0]["category"] == "food"
        assert insights[0]["amount"] == 5000

def test_non_card_transactions_excluded(client, test_db):
    """Test that non-card transactions are not included in insights"""
    today = datetime(2024, 1, 10)
    
    with freeze_time(today):
        # Create two transactions - one card, one cash
        transactions = [
            {
                "customer_id": 1,
                "merchant_id": "merchant-1",
                "amount_cents": 5000,
                "is_card": True  # Card transaction
            },
            {
                "customer_id": 1,
                "merchant_id": "merchant-1",
                "amount_cents": 3000,
                "is_card": False  # Cash transaction
            }
        ]
        
        # Create both transactions
        for t in transactions:
            response = client.post("/transactions", json=t)
            assert response.status_code == 200
        
        # Get insights
        response = client.get("/insights?customer_id=1")
        assert response.status_code == 200
        insights = response.json()
        
        # Verify only card transaction is counted
        assert len(insights) == 1
        assert insights[0]["category"] == "food"
        assert insights[0]["amount"] == 5000  # Only the card transaction amount

def test_all_categories_returned_without_top_n(client, test_db):
    """Test that all categories are returned when top_n is not provided"""
    today = datetime(2024, 1, 10)
    
    with freeze_time(today):
        # Create transactions across all test merchant categories
        transactions = [
            {
                "customer_id": 1,
                "merchant_id": "merchant-1",  # food
                "amount_cents": 5000,
                "is_card": True
            },
            {
                "customer_id": 1,
                "merchant_id": "merchant-2",  # gaming
                "amount_cents": 3000,
                "is_card": True
            },
            {
                "customer_id": 1,
                "merchant_id": "merchant-3",  # utilities
                "amount_cents": 1000,
                "is_card": True
            }
        ]
        
        # Create all transactions
        for t in transactions:
            response = client.post("/transactions", json=t)
            assert response.status_code == 200
        
        # Get insights without top_n parameter
        response = client.get("/insights?customer_id=1")
        assert response.status_code == 200
        insights = response.json()
        
        # Verify all categories are returned
        assert len(insights) == 3  # All three categories
        
        # Verify categories and amounts
        assert insights[0]["category"] == "food"
        assert insights[0]["amount"] == 5000
        assert insights[1]["category"] == "gaming"
        assert insights[1]["amount"] == 3000
        assert insights[2]["category"] == "utilities"
        assert insights[2]["amount"] == 1000
        
        # Verify they're ordered by amount (highest to lowest)
        amounts = [insight["amount"] for insight in insights]
        assert amounts == sorted(amounts, reverse=True)

def test_create_transaction(client, test_db):
    """Test creating a transaction with valid data"""
    today = datetime(2024, 1, 10)
    
    with freeze_time(today):
        # Create a transaction
        transaction = {
            "customer_id": 1,
            "merchant_id": "merchant-1",
            "amount_cents": 5000,
            "is_card": True
        }
        
        # Send POST request
        response = client.post("/transactions", json=transaction)
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert "transaction_id" in data
        transaction_id = data["transaction_id"]
        assert data["message"] == "Transaction created successfully"
        
        # Verify transaction in database directly
        con = sqlite3.connect(test_db)
        cur = con.cursor()
        cur.execute("""
            SELECT customer_id, merchant_id, amount_cents, is_card, date
            FROM transactions
            WHERE id = ?
        """, (transaction_id,))
        
        result = cur.fetchone()
        con.close()
        
        assert result is not None
        customer_id, merchant_id, amount_cents, is_card, date = result
        assert customer_id == "customer-1"
        assert merchant_id == "merchant-1"
        assert amount_cents == 5000
        assert is_card == 1 # SQLite stores boolean as integer
        assert date == today.strftime('%Y-%m-%d')  # Check date matches frozen time

    

