from ketnoisql_server import *
import sqlalchemy
import requests
from api import *
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
import time
import pandas as pd

def fetch_conversations(page_id, access_token, since_time, until_time, max_retries=3, retry_delay=2):
    url = f'https://pages.fm/api/public_api/v1/pages/{page_id}/conversations'
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {
        'page_access_token': access_token,
        'since': since_time,
        'until': until_time,
        'page_number': 1,
        'order_by': 'updated_at'
    }
    all_conversations = []
    
    while True:
        attempt = 0
        while attempt < max_retries:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                api_data = response.json()
                conversations = api_data.get('conversations', [])
                if not conversations:
                    return all_conversations 
                all_conversations.extend(conversations)
                params['page_number'] += 1
                break  
            elif response.status_code == 500:
                attempt += 1
                print(f"Server error (500). Retrying {attempt}/{max_retries}...")
                time.sleep(retry_delay) 
            else:
                print(f"Failed to fetch conversations for page {page_id}. Status code: {response.status_code}")
                return all_conversations  

        if attempt == max_retries:
            print("Max retries reached. Exiting.")
            break
    
    return all_conversations

def extract_ads(ads):
    if isinstance(ads, list) and len(ads) > 0:
        extracted = [f"{ad.get('ad_id', '')}:{ad.get('inserted_at', '')}" 
                     for ad in ads if isinstance(ad, dict) and 'ad_id' in ad and 'inserted_at' in ad]
        return ', '.join(extracted) if extracted else ''
    return ''

def convert_to_date(timestamp):
    if isinstance(timestamp, str):
        for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S'):
            try:
                return datetime.strptime(timestamp, fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
        try:
            return datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d')
        except ValueError:
            return ''
    return ''

def fetch_ids_from_db(schema_name, table_name):
    with engine.connect() as connection:
        query = sqlalchemy.text(f"SELECT id, customer_id, page_id FROM {schema_name}.{table_name}")
        result = connection.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

def conversation_exists(conversation_id, schema_name, table_name):
    query = sqlalchemy.text(f"SELECT 1 FROM {schema_name}.{table_name} WHERE id = :id")
    try:
        with engine.connect() as connection:
            result = connection.execute(query, {'id': conversation_id}).fetchone()
            return result is not None
    except SQLAlchemyError as e:
        print(f"Error checking conversation: {e}")
        return False

def truncate_value(value, max_length):
    if isinstance(value, str):
        return value[:max_length]
    elif isinstance(value, (int, float)):
        return str(value)[:max_length]
    return ''

def insert_conversations(conversations, schema_name, table_name):
    with engine.connect() as connection:
        with connection.begin():
            insert_statement = sqlalchemy.text(f"""
                INSERT INTO {schema_name}.{table_name} 
                (id, inserted_at, ad_ids) 
                VALUES (:id, :inserted_at, :ad_ids)
            """)

            conversations_data = [{
                'id': truncate_value(conv.get('id'), 255),
                'inserted_at': truncate_value(conv.get('inserted_at'), 255),
                'ad_ids': truncate_value(conv.get('ad_ids'), 80000)
            } for conv in conversations]

            connection.execute(insert_statement, conversations_data)

def update_conversation(conversation, schema_name, table_name):
    query = sqlalchemy.text(f"""
        UPDATE {schema_name}.{table_name}
        SET 
            inserted_at = :inserted_at,
            ad_ids = :ad_ids
        WHERE id = :id
    """)

    filtered_conversation = {
        'id': conversation.get('id'),
        'inserted_at': conversation.get('inserted_at'),
        'ad_ids': conversation.get('ad_ids')
    }

    try:
        with engine.connect() as connection:
            transaction = connection.begin()
            result = connection.execute(query, filtered_conversation)
            transaction.commit()
            print(f"Updated {result.rowcount} row(s).")
    except SQLAlchemyError as e:
        print(f"Error updating conversation: {e}")
