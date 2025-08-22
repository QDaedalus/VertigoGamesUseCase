import logging
import os
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras as extras
import pandas as pd
from datetime import datetime, timezone

load_dotenv()
dbpassword = os.getenv('POSTGRES_PASS')
dbuser = os.getenv('POSTGRES_USER')
host_ip = os.getenv('HOST')

df_clans = pd.read_csv('data/clan_sample_data.csv')

def connect_to_db():
    try:
        connection = psycopg2.connect(
        host=host_ip,
        # host="127.0.0.1",
        port="5432",
        database="vertigodb",
        user=dbuser,
        password=dbpassword)
        logging.info("Database connection successful.")
        print("Database connection successful.")
        return connection

    except Exception as e:
        logging.error(f"Database connection failed:{e}")
        print(f"Database connection failed: {e}")
        return None


def column_filterings(df):
    # Date column filtering
    df['created_at'] = df['created_at'].astype(str)
    date_regex = r'^\d{4}-\d{2}-\d{2}'
    df = df[df['created_at'].str.match(date_regex)]
    
    # Region column filtering
    df['region'] = df['region'].astype(str)
    # Sadece 2 harfli (ör: 'EU', 'TR') olanları al
    region_regex = r'^[A-Za-z]{2}$'
    df = df[df['region'].str.match(region_regex)]
    
    df = df[df['name'].notna()]
    return df


def execute_values(conn, df, table):
    df = df.where(pd.notnull(df), None)
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()
    

def get_clan_id(name, region):
    connection = connect_to_db()
    cursor = connection.cursor()
    create_clans = '''SELECT id FROM clans WHERE name = %s AND region = %s;'''
    cursor.execute(create_clans, (name, region)) 
    result = cursor.fetchone()
    cursor.close()
    connection.close()
    return result[0]
    

def clan_creation(data):
    dt = datetime.now(timezone.utc)
    connection = connect_to_db()
    cursor = connection.cursor()
    # Checks if the clan already exists
    check_query = "SELECT 1 FROM clans WHERE name = %s AND region = %s"
    cursor.execute(check_query, (data['name'], data['region']))
    exists = cursor.fetchone()

    if not exists:
        query = "INSERT INTO clans (name, region, created_at) VALUES (%s, %s, %s)"
        cursor.execute(query, (data['name'], data['region'], dt))
        connection.commit()
        message = "Clan created successfully."
    else:
        message = "Clan already exists."

    cursor.close()
    return message
    
def insert_process(df_clans):
    print("DATA INSERTING")
    df_clans = column_filterings(df_clans)
    connection = connect_to_db()
    cursor = connection.cursor()
    create_clans = '''CREATE TABLE IF NOT EXISTS clans(id uuid DEFAULT gen_random_uuid(), name char(20) ,region char(5), created_at TIMESTAMP, PRIMARY KEY (id));'''
    cursor.execute(create_clans)
    sql_trunc_clans = """ TRUNCATE TABLE clans """
    cursor.execute(sql_trunc_clans)
    execute_values(connection, df_clans, 'clans') 
    connection.commit()
    cursor.close()
    print("Data insertion script executed successfully.")


if __name__ == '__main__':
    insert_process(df_clans)
