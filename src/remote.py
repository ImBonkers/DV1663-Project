import mysql.connector
import os
from dotenv import load_dotenv

def main():
    load_dotenv()
    print(os.getenv("DB_PASSWORD"))
    connection = mysql.connector.connect(
        host=os.getenv("DB_IP_ADDRESS"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_DATABASE"),
    )

    cursor = connection.cursor()

    if connection.is_connected():
        print("Connected to MySQL database")
        cursor.execute("""
                       show tables
                       """) 
        result = cursor.fetchall()
        print(result)
    else:
        print("Connection failed")

if __name__ == "__main__":
    main()
