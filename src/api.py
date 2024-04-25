import mysql.connector
import os
from typing import Union
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv

load_dotenv()
connection = mysql.connector.connect(
    host=os.getenv("DB_IP_ADDRESS"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_DATABASE"),
)

app = FastAPI()

@app.get("/")
def read_root():
    return "Imdb Search API"


@app.get("/items/{item_id}")
def read_item(item_id, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


@app.get("/person/{id}")
def get_person(id: str, test: Union[str, None] = None):
    cursor = db.cursor()
    cursor.execute(f"SELECT * FROM people WHERE id = \"{id}\"")
    result = cursor.fetchall()
    db.commit()

    if len(result) == 0:
        raise HTTPException(status_code=404, detail="Item not found")

    return {"person": result[0], "test": test}

@app.get("/people/{name}")
def get_person(name: str):
    name = name.lower()
    name = name.replace(" ", " +")
    cursor = db.cursor()
    cursor.execute(f"SELECT name FROM people WHERE MATCH(name) AGAINST('+{name}' IN BOOLEAN MODE)")
    result = cursor.fetchall()
    db.commit()

    if len(result) == 0:
        raise HTTPException(status_code=404, detail="Item not found")

    return {"people": result}

@app.get("/title/{id}")
def get_title(id: str):
    cursor = db.cursor()
    cursor.execute(f"SELECT * FROM titles WHERE id = \"{id}\"")
    result = cursor.fetchall()
    db.commit()

    if len(result) == 0:
        raise HTTPException(status_code=404, detail="Item not found")

    return {"title": result[0]}

@app.get("/titles/{name}")
def get_title(name: str):
    name = name.lower()
    name = name.replace(" ", " +")
    cursor = db.cursor()
    cursor.execute(f"""
                   SELECT * FROM titles 
                   WHERE MATCH(title)
                   AGAINST('+{name}' IN BOOLEAN MODE)
                   AND type = 'movie'
                   """)
    result = cursor.fetchall()
    db.commit()

    if len(result) == 0:
        raise HTTPException(status_code=404, detail="Item not found")

    return {"titles": result}



