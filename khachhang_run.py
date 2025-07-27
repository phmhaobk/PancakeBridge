import pandas as pd
from ketnoisql_server import *
import sqlalchemy
import requests
from api import *
from datetime import datetime, timedelta
from sqlalchemy.exc import SQLAlchemyError
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler('customer_process.log'),
                        logging.StreamHandler()
                    ])

logger = logging.getLogger()

# Global list to hold log data
log_data_list = []

def fetch_customer_pancake(page_id, access_token, since_time, until_time):
    url = f'https://pages.fm/api/public_api/v1/pages/{page_id}/page_customers'
    page_number = 1
    all_data = []
    
    while True:
        params = {
            "page_access_token": access_token,
            "since": since_time,
            "until": until_time,
            "page_number": page_number,
            "page_size": 100  
        }
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            customers = data.get('customers', [])
            if not customers:
                break  
            
            for customer in customers:
                customer['page_id'] = page_id
            all_data.extend(customers)
            page_number += 1  
        else:
            logger.error(f"Failed to retrieve data for page_id {page_id}: {response.status_code}")
            try:
                error_data = response.json()
                logger.error(f"Error data: {error_data}")
            except ValueError:
                logger.error("No JSON response received")
            break  

    return all_data

def extract_message_notes(notes):
    if isinstance(notes, list) and len(notes) > 0:
        message_notes = [note.get('message', '') for note in notes if isinstance(note, dict) and 'message' in note]
        return ','.join(message_notes) if message_notes else ''
    return ''

def convert_to_date(timestamp):
    if isinstance(timestamp, str):
        try:
            return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d')
        except ValueError:
            try:
                return datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
            except ValueError:
                try:
                    return datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d')
                except ValueError:
                    return ''
    return ''

def extract_phone_number(phone_numbers):
    if isinstance(phone_numbers, list):
        cleaned_numbers = []
        for phone_number in phone_numbers:
            if isinstance(phone_number, str):
                cleaned_number = re.sub(r'[.\s()-]+', '', phone_number)
                
                if cleaned_number.startswith('+84'):
                    cleaned_number = '0' + cleaned_number[3:]
                
                cleaned_numbers.append(cleaned_number)
        return cleaned_numbers
    return []

def convert_recent_orders(recent_orders):
    if isinstance(recent_orders, list):
        return json.dumps(recent_orders)
    return ''

def customer_exists(id, schema_name, table_name):
    query = text(f"SELECT 1 FROM {schema_name}.{table_name} WHERE id = :id")
    try:
        with engine.connect() as connection:
            result = connection.execute(query, {'id': id}).fetchone()
            return result is not None
    except SQLAlchemyError as e:
        logger.error(f"An error occurred while checking if customer exists: {e}")
        return False

def insert_customers(customers, schema_name, table_name):
    with engine.connect() as connection:
        with connection.begin():
            insert_statement = sqlalchemy.text(f"""
                INSERT INTO {schema_name}.{table_name} 
                (id, customer_id, phone_number, name, gender, inserted_at, psid, recent_orders, note_messages, page_id, thread_id) 
                VALUES (:id, :customer_id, :phone_number, :name, :gender, :inserted_at, :psid, :recent_orders, :note_messages, :page_id, :thread_id)
            """)

            customers_with_prefix = [{
                'id': cus.get('id', ''),
                'customer_id': cus.get('customer_id', ''),
                'phone_number': ','.join(cus.get('phone_number', [])) if isinstance(cus.get('phone_number', []), list) else cus.get('phone_number', ''),
                'name': cus.get('name', ''),
                'gender': cus.get('gender', ''),
                'inserted_at': cus.get('inserted_at', ''),
                'psid': cus.get('psid', ''),
                'recent_orders': cus.get('recent_orders', ''),
                'note_messages': cus.get('note_messages', ''),
                'page_id': cus.get('page_id', ''),
                'thread_id': cus.get('thread_id', '')
            } for cus in customers]
            
            connection.execute(insert_statement, customers_with_prefix)
            logger.info(f"Inserted {len(customers_with_prefix)} customers into the database.")
def update_customer(customers, schema_name, table_name, engine, logger):
    update_statement = sqlalchemy.text(f"""
        UPDATE {schema_name}.{table_name}
        SET customer_id = :customer_id, 
            phone_number = :phone_number, 
            name = :name, 
            gender = :gender, 
            inserted_at = :inserted_at, 
            psid = :psid, 
            recent_orders = :recent_orders, 
            note_messages = :note_messages, 
            page_id = :page_id, 
            thread_id = :thread_id
        WHERE id = :id
    """)

    for cus in customers:
        if not isinstance(cus, dict):
            logger.error(f"Expected a dictionary but got: {cus}")
            continue

        customers_with_prefix_ud = {
            'id': cus.get('id', ''),
            'customer_id': cus.get('customer_id', ''),
            'phone_number': ','.join(cus.get('phone_number', [])) if isinstance(cus.get('phone_number', []), list) else cus.get('phone_number', ''),
            'name': cus.get('name', ''),
            'gender': cus.get('gender', ''),
            'inserted_at': cus.get('inserted_at', ''),
            'psid': cus.get('psid', ''),
            'recent_orders': cus.get('recent_orders', ''),
            'note_messages': cus.get('note_messages', ''),
            'page_id': cus.get('page_id', ''),
            'thread_id': cus.get('thread_id', '')
        }
        
        try:
            with engine.connect() as connection:
                connection.execute(update_statement, customers_with_prefix_ud)
                logger.info(f"Updated customer ID {customers_with_prefix_ud['id']} in the database.")
        except SQLAlchemyError as e:
            logger.error(f"An error occurred while updating customer ID {customers_with_prefix_ud['id']}: {e}")

today = datetime.today()
yesterday = today - timedelta(days=3) 
start_time = datetime(yesterday.year, yesterday.month, yesterday.day)
end_time = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)

since_time = int(start_time.timestamp())
until_time = int(end_time.timestamp())

current_time = start_time
while current_time <= end_time:
    since_time = int(current_time.timestamp())
    until_time = int((current_time + timedelta(days=1) - timedelta(seconds=1)).timestamp())
    
    all_customer_data = []

    for page_id in page_ids:
        access_token = page_access_tokens.get(page_id)
        if access_token:
            customers = fetch_customer_pancake(page_id, access_token, since_time, until_time)
            if customers:
                all_customer_data.extend(customers)  

    df_khachhang = pd.DataFrame(all_customer_data)
    logger.info(f"Fetched customer data for date range {datetime.fromtimestamp(since_time).strftime('%Y-%m-%d')} to {datetime.fromtimestamp(until_time).strftime('%Y-%m-%d')}")
    logger.info(f"Data frame contents:\n{df_khachhang.T}")

    def process_customers():
        if not df_khachhang.empty:
            df_khachhang['phone_number'] = df_khachhang['phone_numbers'].apply(extract_phone_number)
            df_khachhang['inserted_at'] = df_khachhang['inserted_at'].apply(convert_to_date)
            df_khachhang['note_messages'] = df_khachhang['notes'].apply(extract_message_notes)
            df_khachhang['recent_orders'] = df_khachhang['recent_orders'].apply(convert_recent_orders)

            logger.info(f"Processing {len(df_khachhang)} customers")

            fromDate = df_khachhang['inserted_at'].min()
            toDate = df_khachhang['inserted_at'].max()

            customers_to_insert = []
            customers_to_update = []

            for index, row in df_khachhang.iterrows():
                customers_data = {
                    'id': row.get('id', ''),
                    'customer_id': row.get('customer_id', ''),
                    'phone_number': row.get('phone_number', None) if row.get('phone_number') else None,
                    'name': row.get('name', ''),
                    'gender': row.get('gender', ''),
                    'inserted_at': row.get('inserted_at', ''),
                    'psid': row.get('psid', ''),
                    'recent_orders': row.get('recent_orders', ''),
                    'note_messages': row.get('note_messages', None) if row.get('note_messages') else None,
                    'page_id': row.get('page_id', ''),
                    'thread_id': row.get('thread_id', '')
                }
                logger.debug(f"Customer data: {customers_data}")
                if customer_exists(customers_data['id'], schema_name_customers, table_name_customers):
                    logger.info(f"Customer ID {customers_data['id']} already exists in the database. Updating...")
                    customers_to_update.append(customers_data)
                else:
                    customers_to_insert.append(customers_data)

            if customers_to_insert:
                insert_customers(customers_to_insert, schema_name_customers, table_name_customers)
                total_inserted = len(customers_to_insert)
                log_entry = {
                    "logDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "databaseName": "stg",
                    "tableName": "pancake_dm_KhachHang",
                    "status": "Success",
                    "fromDate": fromDate if fromDate else "NULL",
                    "toDate": toDate if toDate else "NULL",
                    "Record": total_inserted,
                    "description": f"Insert success {total_inserted} into table {table_name_customers} of {schema_name_customers}"
                }
                log_data_list.append(log_entry)
                logger.info(log_entry)
            
            if customers_to_update:
                # Pass engine and logger to update_customer function
                update_customer(customers_to_update, schema_name_customers, table_name_customers, engine, logger)
                total_updated = len(customers_to_update)
                log_entry = {
                    "logDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "databaseName": "stg",
                    "tableName": "pancake_dm_KhachHang",
                    "status": "Success",
                    "fromDate": fromDate if fromDate else "NULL",
                    "toDate": toDate if toDate else "NULL",
                    "Record": total_updated,
                    "description": f"Update success {total_updated} in table {table_name_customers} of {schema_name_customers}"
                }
                log_data_list.append(log_entry)
                logger.info(log_entry)


    process_customers()
    
    current_time += timedelta(days=1)

def insert_log_data(log_entries, schema_name, table_name):
    with engine.connect() as connection:
        with connection.begin():
            insert_statement = text(f"""
                INSERT INTO {schema_name}.{table_name} 
                (logDate, databaseName, tableName, status, fromDate, toDate, Record, description) 
                VALUES (:logDate, :databaseName, :tableName, :status, :fromDate, :toDate, :Record, :description)
            """)

            try:
                for entry in log_entries:
                    connection.execute(insert_statement, entry)
                logger.info(f"Inserted {len(log_entries)} log entries into the database.")
            except SQLAlchemyError as e:
                logger.error(f"An error occurred while inserting log data: {e}")

# Convert log data to a list of dictionaries for insertion
log_entries = [
    {
        "logDate": entry["logDate"],
        "databaseName": entry["databaseName"],
        "tableName": entry["tableName"],
        "status": entry["status"],
        "fromDate": entry["fromDate"],
        "toDate": entry["toDate"],
        "Record": entry["Record"],
        "description": entry["description"]
    }
    for entry in log_data_list
]


logger.info("Log data inserted into SQL Server table stg.log_KhachHang")