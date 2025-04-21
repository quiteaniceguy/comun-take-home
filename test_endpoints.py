import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient
import sqlite3
from main import app
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
    with patch('main.get_db_name', return_value=TEST_DB):
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

def test_insights_filtering(client, test_db):
    """Test insights with different filters"""
    # Create multiple transactions
    transactions = [
        {
            "customer_id": 1,
            "merchant_id": "merchant-1",
            "amount_cents": 5000,
            "is_card": True
        },
        {
            "customer_id": 1,
            "merchant_id": "merchant-2",
            "amount_cents": 3000,
            "is_card": True
        }
    ]
    
    for t in transactions:
        response = client.post("/transactions", json=t)
        assert response.status_code == 200
    
    # Test top_n parameter
    response = client.get("/insights?customer_id=1&top_n=1")
    assert response.status_code == 200
    insights = response.json()
    assert len(insights) == 1
    assert insights[0]["amount"] == 5000 