import mysql.connector
import os
from typing import Union
from fastapi import FastAPI, Query,  HTTPException
from typing import List
from dotenv import load_dotenv

load_dotenv()
db = mysql.connector.connect(
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


@app.get("/genres/")
def get_movies_with_genres(g: List[str] = Query(None)):
    print("Query: ", g)
    g = [g.title() for g in g]
    list_str = str(g)[1:-1].strip(",")
    list_str = "(" + list_str + ")"
    print("List_str: ", list_str)

    query = "SELECT t.id, t.title\n"
    for idx in range(len(g)):
        query += f", tg{idx}.genre AS genre{idx}\n"
    query += "FROM titles t\n"

    for idx in range(len(g)):
        query += f" JOIN titles_genres tg{idx} ON t.id = tg{idx}.title AND tg{idx}.genre = '{g[idx]}'\n"
    query += "limit 1000"

    print(query)

    cursor = db.cursor()
    cursor.execute(query)
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
def get_ten_specific_prof(prof : str):
    cursor = db.cursor()
    cursor.execute(f"SELECT p.name, prof.profession FROM people p JOIN peoples_professions prof ON p.id = prof.person WHERE prof.profession = '{prof}';")
    result = cursor.fetchall()
    db.commit()

    if len(result) == 0:
        raise HTTPException(status_code=404, detail="Item not found")

    ans = f"amount of {prof}s"

    return {ans : result[0:10]}


"""
Get top 10 youngest actors
"""
@app.get("/youngest_actors")
def get_youngest_actors():
    cursor = db.cursor()
    cursor.execute(f"SELECT * FROM people WHERE death_year = -1 AND birth_year != -1 AND birth_year > 1900 ORDER BY birth_year DESC LIMIT 10;")
    result = cursor.fetchall()
    db.commit()

    if result == 0:
        raise HTTPException(status_code=404, detail="Item not found")

    ans = f"youngest actors"

    return {ans : result}


"""
Get every role that a person has worked as 
"""
# nm0136797 - Steve Carell

@app.get("/person_professions/{person_id}")
def get_person_professions(person_id: str):
    cursor = db.cursor()
    cursor.execute(f"SELECT prof.profession FROM people p JOIN peoples_professions prof ON p.id = prof.person WHERE p.id = '{person_id}';")
    result = cursor.fetchall()
    db.commit()

    if len(result) == 0:
        raise HTTPException(status_code=404, detail="Person not found")

    ans = f"id {person_id} is working as "

    return {ans : result}


@app.get("/genre/{title_id}")
def get_genre_by_id(title_id : str):
    cursor = db.cursor()
    cursor.execute(f"SELECT genre FROM titles_genres WHERE title = '{title_id}';")
    result = cursor.fetchall()
    db.commit()

    if len(result) == 0:
        raise HTTPException(status_code=404, detail="Person not found")

    ans = f"id {title_id} is a "

    return {ans : result}





"""
@app.get("/genre/{genre}")
def get_person_professions(genre : list):

    result = []

    for item in genre:
        cursor = db.cursor()
        cursor.execute(f"SELECT * FROM titles_genre WHERE genre = '{item}';")
        result.append(cursor.fetchall())
        db.commit()

        if len(result) == 0:
            raise HTTPException(status_code=404, detail="Person not found")

        ans = f"id {genre} is working as "

    return {ans : result}
"""
    

