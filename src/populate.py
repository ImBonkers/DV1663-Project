import io
import os
import mysql.connector
from dotenv import load_dotenv
from tqdm import tqdm
from itertools import islice

def drop_tables(connection):
    cursor = connection.cursor()

    print("Dropping tables")
    print("Dropping titles_people table")
    cursor.execute("DROP TABLE IF EXISTS titles_people")

    print("Dropping titles_genres table")
    cursor.execute("DROP TABLE IF EXISTS titles_genres")

    print("Dropping titles table")
    cursor.execute("DROP TABLE IF EXISTS titles")

    print("Dropping peoples_professions table")
    cursor.execute("DROP TABLE IF EXISTS peoples_professions")

    print("Dropping people table")
    cursor.execute("DROP TABLE IF EXISTS people")

    connection.commit()

def setup_tables(connection):
    cursor = connection.cursor()

    print("Creating tables...")
    print("Creating names table...")
    cursor.execute("""CREATE TABLE IF NOT EXISTS 
                   people (
                       id               VARCHAR(20) PRIMARY KEY,
                       name             VARCHAR(300) NOT NULL,
                       birth_year       INT DEFAULT -1,
                       death_year       INT DEFAULT -1,
                       FULLTEXT(name),
                       INDEX idx_id (id),
                       INDEX idx_name (name)
                    )
                   CHARACTER SET utf8mb4
                   COLLATE utf8mb4_bin
                   """)

    print("Creating titles table...")
    cursor.execute(""" CREATE TABLE IF NOT EXISTS
                   titles (
                       id               VARCHAR(20) PRIMARY KEY,
                       type             VARCHAR(20) NOT NULL,
                       title            VARCHAR(300) NOT NULL,
                       original_title   VARCHAR(300) DEFAULT "",
                       is_adult         BOOLEAN DEFAULT FALSE,
                       start_year       INT DEFAULT -1,
                       runtime          INT DEFAULT -1,
                       FULLTEXT(title),
                       FULLTEXT(original_title),
                       INDEX idx_id (id),
                       INDEX idx_title (title),
                       INDEX idx_original_title (original_title),
                       INDEX idx_type (type)
                    )
                   CHARACTER SET utf8mb4
                   COLLATE utf8mb4_bin
                   """)

    print("Creating junction tables...")
    print("Creating person & movie junction table...")
    cursor.execute(""" CREATE TABLE IF NOT EXISTS
                    titles_people (
                        id              INT AUTO_INCREMENT PRIMARY KEY,
                        role            VARCHAR(40) NOT NULL,
                        char_name       VARCHAR(40) DEFAULT "",
                        person          VARCHAR(20) NOT NULL,
                        title           VARCHAR(20) NOT NULL,
                        FULLTEXT(char_name),
                        FOREIGN KEY (person) REFERENCES people(id),
                        FOREIGN KEY (title)  REFERENCES titles(id),
                        INDEX idx_role (role),
                        INDEX idx_char_name (char_name),
                        INDEX idx_person (person),
                        INDEX idx_title (title)
                    )
                   CHARACTER SET utf8mb4
                   COLLATE utf8mb4_bin
                   """)

    print("Creating movie & genre junction table...")
    cursor.execute(""" CREATE TABLE IF NOT EXISTS
                   titles_genres (
                       id               INT AUTO_INCREMENT PRIMARY KEY,
                       genre            VARCHAR(40) NOT NULL,
                       title            VARCHAR(20) NOT NULL,
                       FOREIGN KEY (title) REFERENCES titles(id),
                       INDEX idx_genre (genre),
                       INDEX idx_title (title)
                    )
                   CHARACTER SET utf8mb4
                   COLLATE utf8mb4_bin
                   """)

    print("Creating person & profession junction table...")
    cursor.execute(""" CREATE TABLE IF NOT EXISTS
                   peoples_professions (
                       id               INT AUTO_INCREMENT PRIMARY KEY,
                       profession       VARCHAR(40) NOT NULL,
                       person           VARCHAR(20) NOT NULL,
                       FOREIGN KEY (person) REFERENCES people(id),
                       INDEX idx_profession (profession),
                       INDEX idx_person (person)
                    )
                   CHARACTER SET utf8mb4
                   COLLATE utf8mb4_bin
                   """)

def fill_names_table(data, connection):
    print("Inserting data into Person tables")

    cursor = connection.cursor()


    people_query = f"""INSERT IGNORE INTO
                       people (
                               id,
                               name,
                               birth_year,
                               death_year
                               )
                       VALUES (%s, %s, %s, %s)
                       """

    profession_query = f"""INSERT IGNORE INTO
                            peoples_professions (
                                    id,
                                    person,
                                    profession
                                    )
                            VALUES (%s, %s, %s)
                        """

    backup_counter = 0

    people_entries = []
    profession_entries = []

    surrogate_counter = 0
    for entry in tqdm(data, mininterval=1):
        if backup_counter >= 100000:
            cursor.executemany(people_query, people_entries)
            cursor.executemany(profession_query, profession_entries)
            connection.commit()
            people_entries = []
            profession_entries = []
            backup_counter = 0

        fields = entry.split("\t")
        nconst = fields[0]
        primary_name = fields[1]
        birth_year = int(fields[2]) if fields[2] != "\\N" else -1
        death_year = int(fields[3]) if fields[3] != "\\N" else -1
        professions = fields[4].split(",")

        primary_name = primary_name.replace("\"", "")
        primary_name = primary_name.replace("\'", "")

        people_entries.append((nconst, primary_name, birth_year, death_year))

        for profession in professions:
            profession_entries.append((surrogate_counter, nconst, profession))
            surrogate_counter += 1

        backup_counter += 1
    
    cursor.executemany(people_query, people_entries)
    cursor.executemany(profession_query, profession_entries)
    connection.commit()


def fill_titles_table(data, connection):

    cursor = connection.cursor()

    titles_query = f"""INSERT IGNORE INTO
                       titles (
                               id,
                               type,
                               title,
                               original_title,
                               is_adult,
                               start_year,
                               runtime
                               )
                       VALUES (%s, %s, %s, %s, %s, %s, %s)
                       """

    genre_query = f"""INSERT IGNORE INTO
                    titles_genres (
                            id,
                            genre,
                            title
                            )
                    VALUES (%s, %s, %s)
                    """

    title_entries = []
    genre_entries = []
    backup_counter = 0
    surrogate_counter = 0

    for entry in tqdm(data, mininterval=1):
        if backup_counter >= 100000:
            cursor.executemany(titles_query, title_entries)
            cursor.executemany(genre_query, genre_entries)
            connection.commit()
            title_entries = []
            genre_entries = []
            backup_counter = 0

        fields = entry.split("\t")
        tconst = fields[0]
        title_type = fields[1]
        primary_title = fields[2]
        original_title = fields[3]
        is_adult = bool(fields[4])
        start_year = int(fields[5]) if fields[5] != "\\N" else -1
        runtime = int(fields[7]) if fields[7] != "\\N" else -1
        genres = fields[8].split(",")

        primary_title = primary_title.replace("\"", "")
        primary_title = primary_title.replace("\'", "")
        original_title = original_title.replace("\"", "")
        original_title = original_title.replace("\'", "")

        title_entry = (
            tconst, 
            title_type, 
            primary_title, 
            original_title, 
            is_adult, 
            start_year, 
            runtime
            )

        title_entries.append(title_entry)

        for genre in genres:
            genre_entry = (surrogate_counter, genre, tconst)
            genre_entries.append(genre_entry)
            surrogate_counter += 1

        backup_counter += 1

    cursor.executemany(titles_query, title_entries)
    cursor.executemany(genre_query, genre_entries)
    connection.commit()


def fill_titles_people_table(data, connection):
    
    cursor = connection.cursor()

    titles_people_query = f"""INSERT IGNORE INTO
                            titles_people (
                                    id,
                                    role,
                                    char_name,
                                    person,
                                    title
                                    )
                            VALUES (%s, %s, %s, %s, %s)
                            """

    title_people_entries = []
    backup_counter = 0
    surrogate_counter = 0

    for entry in tqdm(data, mininterval=1):
        if backup_counter >= 100000:
            cursor.executemany(titles_people_query, title_people_entries)
            connection.commit()
            title_people_entries = []
            backup_counter = 0

        fields = entry.split("\t")
        tconst = fields[0]
        nconst = fields[2]
        role = fields[3] if fields[3] != "\\N" else ""
        character = fields[5].split(",") if fields[5] != "\\N" else []

        if len(character) > 1:
            for c in character:
                entry = (surrogate_counter, role, c, nconst, tconst)
                title_people_entries.append(entry)
                surrogate_counter += 1
                backup_counter += 1
        else:
            entry = (surrogate_counter, role, "", nconst, tconst)
            title_people_entries.append(entry)
            surrogate_counter += 1
            backup_counter += 1


    cursor.executemany(titles_people_query, title_people_entries)
    connection.commit()



def main():

    """
    connection = mysql.connector.connect(
           host="localhost",
           user="root",
           password="password",
           database="imdb",
           )
    """

    load_dotenv()
    connection = mysql.connector.connect(
        host=os.getenv("DB_IP_ADDRESS"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_DATABASE"),
    )

    #drop_tables(connection)

    setup_tables(connection)

    data = open("../data/name.basics.tsv", "r")
    data = data.readlines()
    data = data[8000000:]
    fill_names_table(data, connection)
    
    data = open("../data/title.basics.tsv", "r")
    data = data.readlines()
    data = data[1:]
    fill_titles_table(data, connection)


    data = open("../data/title.principals.tsv", "r")
    data = data.readlines()
    data = data[1:]
    fill_titles_people_table(data, connection)

    print("Done")
    connection.close()

if __name__ == '__main__':
    main()
