from dotenv import load_dotenv
import pandas as pd
import psycopg2
import os

def get_database_connection():
    load_dotenv()
    conn = psycopg2.connect(
        host=os.getenv("DATABASE_HOST", "localhost"),
        port=os.getenv("DATABASE_PORT", 5432),
        dbname=os.getenv("DATABASE_NAME", "spark_streaming_db"),
        user=os.getenv("DATABASE_USER", "divinandretomadam"),
        password=os.getenv("DATABASE_PASSWORD", "oDAnmvidrTnmeiAa")
    )
    return conn

def close_database_connection(conn):
    conn.close()

def query_db(query):
    try:
        conn = get_database_connection()
        df = pd.read_sql_query(query, conn)
        close_database_connection(conn)
        return df
    except Exception as e:
        raise Exception("Error lors de la recuperation de données", e)
