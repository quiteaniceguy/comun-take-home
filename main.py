from fastapi import FastAPI
import sqlite3
from pydantic import BaseModel

app = FastAPI()


@app.get("/")
async def root():
    con = sqlite3.connect("tutorial.db")
    cur = con.cursor()
    cur.execute("CREATE TABLE users(name, age)")
    cur.execute("INSERT INTO users (name, age) VALUES ('John', 25)")
    con.commit()
    con.close
    return {"message": "Hello World"}


@app.get("/users")
async def get_users():
    con = sqlite3.connect("tutorial.db")
    cur = con.cursor()
    cur.execute("SELECT * FROM users")
    users = cur.fetchall()
    return users

class UserCreate(BaseModel):
    name: str
    age: int

@app.post("/users")
async def create_user(input: UserCreate):
    con = sqlite3.connect("tutorial.db")
    cur = con.cursor()
    cur.execute("INSERT INTO users (name, age) VALUES (?, ?)", (input.name, input.age))
    con.commit()
    con.close()
    return {"message": f"User {input.name} created successfully"}


