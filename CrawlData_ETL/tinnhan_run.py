import pandas as pd
from ketnoisql_server import *
from crawl_tinnhan_pancake import *
import logging
from datetime import datetime

import logging

# Cấu hình logging với UTF-8 encoding
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler('process_conversations_tn.log', encoding='utf-8'),  # Ensure the log file uses UTF-8
                        logging.StreamHandler()  # Console logging still uses default encoding, but can be fixed by setting the console encoding
                    ])

logger = logging.getLogger()


log_data = []

date_format_full = '%Y-%m-%d %H:%M:%S'
date_format_simple = '%Y-%m-%d'


def insert_messages(messages, schema_name, table_name):
   
    insert_statement = sqlalchemy.text(f"""
        INSERT INTO {schema_name}.{table_name} 
        (conversation_id, customer_id, inserted_at, current_count_messages, messages) 
        VALUES (:conversation_id, :customer_id, :inserted_at, :current_count_messages, :messages)
    """)

    messages_with_prefix = [{
        'conversation_id': msg.get('conversation_id', ''),
        'customer_id': msg.get('customer_id', ''),
        'inserted_at': msg.get('inserted_at', ''),
        'current_count_messages': msg.get('current_count_messages', 0),
        'messages': msg.get('messages', '')  
    } for msg in messages]

    with engine.connect() as connection:
        with connection.begin():
            for msg in messages_with_prefix:
                try:
                    connection.execute(insert_statement, msg)
                except IntegrityError as e:
                    
                    logger.error(f"Error inserting message {msg}: {e.orig}")
                except Exception as e:
                    
                    logger.error(f"Unexpected error inserting message {msg}: {e}")

    logger.info(f"Data successfully inserted into SQL Server table '{schema_name}.{table_name}'")



def process_conversations():
    df = fetch_ids_from_db(view_name)
    logger.info(f"Fetched conversations from DB: {df}")

    total_conversations = 0
    total_full_conversations = 0
    first_updated_at = None
    last_updated_at = None

    for index, row in df.iterrows():
        conversation_id = row['id']
        customer_id = row['customer_id']
        page_id = row['page_id']
        current_count_message = row['current_count_message']
        message_count = row['message_count']
        last_update = row['last_update']
        updated_at = row['updated_at']
        
        last_update = pd.to_datetime(last_update)

        if first_updated_at is None:
            first_updated_at = updated_at
        last_updated_at = updated_at

        soHoiThoaiLayTN = 0
        soHoiThoaiDaLayHetTN = 0

        if current_count_message == 0:
            new_messages_info = fetch_messages_for_conversation(page_id, customer_id, conversation_id, message_count)
            
            new_messages = new_messages_info.get('messages', [])
            current_count_message = new_messages_info.get('current_count_message', 0)
            soHoiThoaiLayTN += 1
            if current_count_message >= message_count:
                soHoiThoaiDaLayHetTN += 1

            if new_messages:
                all_messages = []
                grouped_messages = format_and_group_messages(new_messages, page_id)
                for msg in grouped_messages:
                    inserted_at = msg.get('inserted_at', '')
                    all_messages.append({
                        'conversation_id': conversation_id,
                        'customer_id': customer_id,
                        'inserted_at': inserted_at,
                        'current_count_messages': len(new_messages),
                        'messages': msg.get('messages', '')
                    })

                if all_messages:
                    df_messages = pd.DataFrame(all_messages)
                    
                    logger.info(f"\nData Frame for conversation {conversation_id}:\n{df_messages.T}")

                    messages_to_insert = [
                        {
                            'conversation_id': row['conversation_id'],
                            'customer_id': row['customer_id'],
                            'inserted_at': row['inserted_at'],
                            'current_count_messages': row['current_count_messages'],
                            'messages': row['messages']
                        }
                        for index, row in df_messages.iterrows()
                    ]
                    insert_messages(messages_to_insert, schema_name_messages, table_name_messages)
                    
            update_conversation(schema_name_conversations, table_name_conversations, conversation_id, current_count_message)
            logger.info(f"Updated conversation {conversation_id} in DB")

            total_conversations += 1
            if current_count_message >= message_count:
                total_full_conversations += 1
            
        elif current_count_message < message_count:
            current_count_message_d = current_count_message
            print(current_count_message_d)        
            new_messages_info = fetch_messages_for_conversation(page_id, customer_id, conversation_id, message_count)
            
            new_messages = new_messages_info.get('messages', [])
            current_count_message = new_messages_info.get('current_count_message', 0)


            if new_messages:
                    all_messages = []
                    grouped_messages = format_and_group_messages(new_messages, page_id)
                    for msg in grouped_messages:
                        inserted_at_str = msg.get('inserted_at', '')
                        try:
                            inserted_at = datetime.strptime(inserted_at_str, date_format_full) if inserted_at_str else None
                        except ValueError:
                            inserted_at = datetime.strptime(inserted_at_str, date_format_simple) if inserted_at_str else None
                        
                        inserted_at = pd.to_datetime(inserted_at) if inserted_at else None

                        all_messages.append({
                            'conversation_id': conversation_id,
                            'customer_id': customer_id,
                            'inserted_at': inserted_at,
                            'current_count_messages': len(new_messages),
                            'messages': msg.get('messages', '')
                        })

                    if all_messages:
                        df_messages = pd.DataFrame(all_messages)
                        
                        logger.info(f"\nData Frame for conversation {conversation_id}:\n{df_messages.T}")

                        df1 = fetch_inserted_at_done(schema_name_messages, table_name_messages, conversation_id)
                        
                        df_messages['inserted_at'] = pd.to_datetime(df_messages['inserted_at'])
                        df1['inserted_at'] = pd.to_datetime(df1['inserted_at'])
                        
                        filtered_messages = df_messages[~df_messages['inserted_at'].isin(df1['inserted_at'])]
                        
                        valid_message_count = 0
                        messages_to_insert = []
                        
                        for index, row in filtered_messages.iterrows():
                            message_list = row['messages'].split('||')
                            message_list = [msg.strip() for msg in message_list if msg.strip() and not msg.strip().startswith('-<3-')]
                            message_count = len(message_list)
                            
                            valid_message_count += message_count

                            current_count_message_2 =current_count_message_d + valid_message_count

                            messages_to_insert.append({
                                'conversation_id': row['conversation_id'],
                                'customer_id': row['customer_id'],
                                'inserted_at': row['inserted_at'],
                                'current_count_messages': current_count_message_2,
                                'messages': row['messages']
                            })

                        if messages_to_insert:
                            insert_messages(messages_to_insert, schema_name_messages, table_name_messages)
                            update_conversation(schema_name_conversations, table_name_conversations, conversation_id, current_count_message_2)
                            logger.info(f"Updated conversation {conversation_id} in DB")
                        
                            total_conversations += 1
                            if current_count_message_2 >= valid_message_count:
                                total_full_conversations += 1
                
    log_entry = {
        "logDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "soHoiThoaiLayTN": total_conversations,
        "soHoiThoaiDaLayHetTN": total_full_conversations,
        "fromDate": first_updated_at.strftime("%Y-%m-%d") if first_updated_at else None,
        "toDate": last_updated_at.strftime("%Y-%m-%d") if last_updated_at else None,
        "status": "Success"
    }
    
process_conversations()