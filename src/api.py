from typing import Union
from fastapi import FastAPI, HTTPException
import mysql.connector


"""
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="imdb"
)
"""

db = mysql.connector.connect(
    host="158.179.207.178",
    user="dv1663",
    password="dv1663password123",
    database="DV1663",
)


app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "world"}


@app.get("/items/{item_id}")
def read_item(item_id, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


@app.get("/people_id/{id}")
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




"""
Get the amount of people with specific profession
"""
@app.get("/professions_amount/{prof}")
def get_writers(prof: str):
    cursor = db.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM people p JOIN peoples_professions prof ON p.id = prof.person WHERE prof.profession = '{prof}';")
    result = cursor.fetchone()[0]  # Fetch the count directly
    db.commit()

    if result == 0:
        raise HTTPException(status_code=404, detail="Item not found")

    ans = f"amount of {prof}s"

    return {ans: result}

"""
(!) JÄMFÖR VILKEN SOM ÄR SNABBAST
@app.get("/professions_amount/{prof}")
def get_writers(prof : str):
    cursor = db.cursor()
    cursor.execute(f"SELECT p.name, prof.profession FROM people p JOIN peoples_professions prof ON p.id = prof.person WHERE prof.profession = '{prof}';")
    result = cursor.fetchall()
    db.commit()

    if len(result) == 0:
        raise HTTPException(status_code=404, detail="Item not found")

    ans = f"amount of {prof}s"

    return {ans : len(result)}
"""


"""
Get first 10 people with specific job
"""
@app.get("/profession/{prof}")
def get_writers(prof : str):
    cursor = db.cursor()
    cursor.execute(f"SELECT p.name, prof.profession FROM people p JOIN peoples_professions prof ON p.id = prof.person WHERE prof.profession = '{prof}';")
    result = cursor.fetchall()
    db.commit()

    if len(result) == 0:
        raise HTTPException(status_code=404, detail="Item not found")

    ans = f"amount of {prof}s"

    return {ans : result[0:10]}

