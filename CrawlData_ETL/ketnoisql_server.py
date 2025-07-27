import sqlalchemy as sa
import pandas as pd
import urllib
from sqlalchemy import text
import json
import pyodbc

with open('config.json', 'r') as file:
    config = json.load(file)

db_config = config['connection']

server = db_config['server']
database = db_config['database']
username = db_config['username']
password = db_config['password']

params = urllib.parse.quote_plus(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};charset=utf8')
connection_str = f'mssql+pyodbc:///?odbc_connect={params}'

engine = sa.create_engine(connection_str)

try:
    with engine.connect() as connection:
        print("Connection successful!")
except Exception as e:
    print(f"Error connecting to the database: {e}")

pancake_config = config['pancake']

table_name_conversations = pancake_config['table']['conversations']
table_name_messages = pancake_config['table']['conversation_mess']
table_name_customers = pancake_config['table']['customer']
table_name_dm_tags = pancake_config['table']['tags']

schema_name_conversations = pancake_config['schema']
schema_name_messages = pancake_config['schema']
schema_name_customers = pancake_config['schema']

view_name = pancake_config['view']['stg.view_pancake_messages']