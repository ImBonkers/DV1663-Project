import mysql.connector
import os
from typing import Union, List
from fastapi import FastAPI, Query,  HTTPException
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()
db = mysql.connector.connect(
    host=os.getenv("DB_IP_ADDRESS"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_DATABASE"),
)


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

@app.get("/")
def read_root():
    return "Imdb Search API"


@app.get("/people_id/{id}")
def get_person(id: str, test: Union[str, None] = None):
    cursor = db.cursor()
    cursor.execute(f"SELECT * FROM people WHERE id = \"{id}\"")
    result = cursor.fetchall()
    db.commit()

    if len(result) == 0:
        raise HTTPException(status_code=404, detail="Item not found")

    return {"header":"people_id", "data": result}

@app.get("/people/{name}")
def get_person(name: str):
    name = name.replace(" ", " +")
    cursor = db.cursor()
    print(name)
    cursor.execute(f"SELECT id, name FROM people WHERE MATCH(name) AGAINST(\"{name}\" IN BOOLEAN MODE) limit 1000")
    result = cursor.fetchall()
    db.commit()


    if len(result) == 0:
        raise HTTPException(status_code=404, detail="Item not found")

    return {"header":"people", "data":result}

@app.get("/title/{id}")
def get_title(id: str):
    cursor = db.cursor()
    cursor.execute(f"SELECT * FROM titles WHERE id = \"{id}\"")
    result = cursor.fetchall()
    db.commit()

    if len(result) == 0:
        raise HTTPException(status_code=404, detail="Item not found")

    return {"header":"title", "data":result}

@app.get("/titles/{name}")
def get_title(name: str):
    name = name.replace(" ", " +")
    cursor = db.cursor()
    cursor.execute(f"""
                   SELECT * FROM titles 
                   WHERE MATCH(title)
                   AGAINST(\"{name}\" IN BOOLEAN MODE)
                   """)
    result = cursor.fetchall()
    db.commit()

    if len(result) == 0:
        raise HTTPException(status_code=404, detail="Item not found")

    return {"header":"titles", "data":result}


@app.get("/person/movie_count/{id}")
def get_movie_count_by_person(id: str):
    cursor = db.cursor()
    cursor.execute(f"SELECT count_movies_for_person_id(\"{id}\")")
    result = cursor.fetchall()
    db.commit()

    if result == 0:
        raise HTTPException(status_code=404, detail="Item not found")

    return {"header":"count", "data":result}


@app.get("/genres/")
def get_titles_by_genres(
    g: List[str] = Query(None),
    start: Union[str, None] = 0,
    end: Union[str, None] = 1000):

    g = [g.title() for g in g]
    print(g)
    list_str = ""
    for genre in g:
        list_str += genre + ", "
    list_str = list_str[:-2]

    cursor = db.cursor()
    cursor.callproc("GetTitlesByGenres", [list_str, start, end])
    db.commit()

    # Procedures store results in an iterator, since GetTItlesByGenres
    # only has one result we need to get that from the iterator
    data = cursor.stored_results()
    result = next(data).fetchall()

    if len(result) == 0:
        result = [[]]

    return {"header":"titles by genre(s)", "data":result}




"""
Get the amount of people with specific profession
"""
@app.get("/professions_amount/{prof}")
def get_amount_profession(prof: str):
    cursor = db.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM people p JOIN peoples_professions prof ON p.id = prof.person WHERE prof.profession = '{prof}';")
    result = cursor.fetchone()[0]  # Fetch the count directly
    db.commit()

    if result == 0:
        raise HTTPException(status_code=404, detail="Item not found")

    ans = f"amount of {prof}s"

    return {"header": ans, "data": [[result]]}



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

    return {"header": ans, "data": result[0:10]}


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

    return {"header": ans, "data": result}


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

    return {"header": ans, "data": result}


@app.get("/genre/{title_id}")
def get_genre_by_title_id(title_id : str):
    cursor = db.cursor()
    cursor.execute(f"SELECT genre FROM titles_genres WHERE title = '{title_id}';")
    result = cursor.fetchall()
    db.commit()

    if len(result) == 0:
        raise HTTPException(status_code=404, detail="Person not found")

    ans = f"id {title_id} is a "

    return {"header": ans, "data": result}

