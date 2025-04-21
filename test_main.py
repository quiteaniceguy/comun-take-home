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
    with patch('app.main.get_db_name', return_value=TEST_DB):
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