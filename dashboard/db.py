import mysql.connector
import pandas as pd


def get_connection():
    return mysql.connector.connect(
        host="localhost",
        port=3306,
        user="ananth",
        password="Ananth@2003",
        database="banking_db"
    )


def run_query(query):
    connection = get_connection()

    try:
        df = pd.read_sql(query, connection)
        return df
    finally:
        connection.close()
