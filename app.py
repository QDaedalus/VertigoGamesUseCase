import os
import psycopg2
from dotenv import load_dotenv
from flask import Flask
from flask import request
# from flask_sqlalchemy import SQLAlchemy
from os import environ
import json
import pandas as pd
import psycopg2.extras as extras
import logging
from flask import jsonify
from insert_data import connect_to_db, get_clan_id, clan_creation
from datetime import datetime, timezone
load_dotenv()

app = Flask(__name__)

print("STARTED")
dbpassword = os.getenv('POSTGRES_PASS')
dbuser = os.getenv('POSTGRES_USER')
host_ip = os.getenv('HOST')


@app.get('/')
def home():
    return "Index Page"


@app.get('/select')
def select():
    return "Select Page"



@app.route('/clans', methods=['GET', 'POST'])
def clans():
    if request.method == 'POST':
        data = request.get_json()
        message = clan_creation(data)
        clan_id = get_clan_id(data['name'], data['region'])

        return jsonify(
            id=clan_id,
            message=message
        )
    if request.method == 'GET':
        connection = connect_to_db()
        cursor = connection.cursor()
        select_query = "SELECT DISTINCT(name),created_at FROM clans ORDER BY created_at DESC"
        cursor.execute(select_query)
        clan_list = cursor.fetchall()
        clan_list = [row[0].strip() for row in clan_list]
        return {'clans':clan_list}
 
        
@app.route('/clans/<clan_id>',methods=['GET','DELETE'])
def clan_detail(clan_id):
    if request.method == 'GET':
        connection = connect_to_db()
        cursor = connection.cursor()
        select_query = "SELECT name,region,created_at FROM clans WHERE id::text = %s"
        cursor.execute(select_query, (clan_id,))
        clan_info = cursor.fetchall()
        print(clan_info)
        if len(clan_info) == 0:
            return jsonify({"error": "Clan not found"}), 404
        else:
            clan_info = [row[0].strip() for row in clan_info]
        # print(select_query)
            return clan_info
            
    elif request.method == 'DELETE':
        connection = connect_to_db()
        cursor = connection.cursor()
        delete_query = "DELETE FROM clans WHERE id::text = %s"
        cursor.execute(delete_query, (clan_id,))
        connection.commit()
        return f"Clan with ID {str(clan_id)} deleted successfully."
    


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)