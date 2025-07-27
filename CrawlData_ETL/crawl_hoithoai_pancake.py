from ketnoisql_server import *
import sqlalchemy
import requests
from api import *
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
import re
import time

def fetch_conversations(page_id, access_token, since_time, until_time, max_retries=3, retry_delay=2):
    url = f'https://pages.fm/api/public_api/v1/pages/{page_id}/conversations'
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
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
                print(f"Server error (500) occurred. Retrying {attempt}/{max_retries}...")
                time.sleep(retry_delay) 
            else:
                print(f"Failed to fetch conversations for page {page_id}. Status code: {response.status_code}")
                print(f"Response content: {response.content}")
                return all_conversations  

        if attempt == max_retries:
            print("Max retries reached. Exiting.")
            break
    
    return all_conversations

def save_to_csv(data):
    data.to_csv(index=False)

def extract_phone_number(recent_phone_numbers):
    if isinstance(recent_phone_numbers, list) and len(recent_phone_numbers) > 0:
        raw_number = recent_phone_numbers[0].get('captured', '')
        
        cleaned_number = re.sub(r'[.\s()-]+', '', raw_number)
        
        if cleaned_number.startswith('+84'):
            cleaned_number = '0' + cleaned_number[3:]
        
        return cleaned_number
    return ''


def extract_customers_fb_id(customers):
    if isinstance(customers, list) and len(customers) > 0:
        return customers[0].get('fb_id', '')
    return ''   

def extract_customers_name(customers):
    if isinstance(customers, list) and len(customers) > 0:
        return customers[0].get('name', '')
    return ''   

def extract_assign_user_fb_id(current_assign_users):
    if isinstance(current_assign_users, list) and len(current_assign_users) > 0:
        first_user = current_assign_users[0]
        if first_user is not None and isinstance(first_user, dict):
            return first_user.get('fb_id', '')
    return ''


def extract_global_id(page_customer):
    if isinstance(page_customer, dict):
        return page_customer.get('global_id', '')
    return ''

def extract_ad_ids(ad_ids):
    if isinstance(ad_ids, list):
        valid_ad_ids = [str(ad_id) for ad_id in ad_ids if isinstance(ad_id, (str, int))]
        return ', '.join(valid_ad_ids)
    return ''



def extract_tags_text(tags):
    if isinstance(tags, list) and len(tags) > 0:
        texts = [tag.get('text', '') for tag in tags if isinstance(tag, dict) and 'text' in tag]
        return ','.join(texts) if texts else ''
    return ''

def extract_tags_id(tags):
    
    if isinstance(tags, list) and len(tags) > 0:
        
        ids = [tag.get('id', '') for tag in tags if isinstance(tag, dict) and tag]
        
        if ids:
            return ids
    
    return None

def extract_tag_histories_id(tag_histories):
    if isinstance(tag_histories, list) and len(tag_histories) > 0:
        ids = [
            tag['payload']['tag'].get('id', '') 
            for tag in tag_histories 
            if isinstance(tag, dict) 
            and 'payload' in tag 
            and 'tag' in tag['payload']
            and isinstance(tag['payload']['tag'], dict)
            and tag['payload']['tag'].get('id') is not None
        ]
        return ','.join(map(str, ids)) if ids else ''
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

def fetch_ids_from_db(schema_name, table_name):
    with engine.connect() as connection:
        query = text(f"SELECT id, customer_id, page_id FROM {schema_name}.{table_name}")
        result = connection.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

def conversation_exists(conversation_id, schema_name, table_name):
    query = text(f"SELECT * FROM {schema_name}.{table_name} WHERE id = :id")
    try:
        with engine.connect() as connection:
            result = connection.execute(query, {'id': conversation_id}).fetchone()
            return result is not None
    except SQLAlchemyError as e:
        print(f"An error occurred: {e}")
        return False

def insert_conversations(conversations, schema_name, table_name):
    def truncate_value(value, max_length):
        if isinstance(value, str) and len(value) > max_length:
            return value[:max_length]
        return value

    with engine.connect() as connection:
        with connection.begin():
            insert_statement = sqlalchemy.text(f"""
                INSERT INTO {schema_name}.{table_name} 
                (page_id, post_id, id, assign_user_id, customer_id, customer_fb_id, has_phone, inserted_at, updated_at, message_count, tags_id, tag_histories_id, type, phone_number, customer_name, current_count_message, is_got_oldest_Messages, tags_text, global_id, ad_ids) 
                VALUES (:page_id, :post_id, :id, :assign_user_id, :customer_id, :customer_fb_id, :has_phone, :inserted_at, :updated_at, :message_count, :tags_id, :tag_histories_id, :type, :phone_number, :customer_name, :current_count_message, :is_got_oldest_Messages, :tags_text, :global_id, :ad_ids)
            """)

            conversations_with_prefix = [{
                'page_id': truncate_value(conv['page_id'], 255),
                'post_id': truncate_value(conv['post_id'], 255),
                'id': truncate_value(conv['id'], 255),
                'assign_user_id': truncate_value(conv['assign_user_id'], 255),
                'customer_id': truncate_value(conv['customer_id'], 255),
                'customer_fb_id': truncate_value(conv['customer_fb_id'], 255),
                'has_phone': conv['has_phone'],
                'inserted_at': conv['inserted_at'],
                'updated_at': conv['updated_at'],
                'message_count': conv['message_count'],
                'tags_id': truncate_value(conv['tags_id'], 80000),  # VARCHAR(MAX) nhưng bạn có thể đặt giới hạn
                'tag_histories_id': truncate_value(conv['tag_histories_id'], 80000),  # VARCHAR(MAX)
                'type': truncate_value(conv['type'], 50),
                'phone_number': truncate_value(conv['phone_number'], 50),
                'customer_name': truncate_value(conv['customer_name'], 255),
                'current_count_message': conv.get('current_count_message', 0),  # Default 0 nếu không có
                'is_got_oldest_Messages': conv.get('is_got_oldest_Messages', False),
                'tags_text': truncate_value(conv['tags_text'], 80000),
                'global_id': truncate_value(conv['global_id'], 255),
                'ad_ids': truncate_value(conv['ad_ids'], 80000)
            } for conv in conversations]

            connection.execute(insert_statement, conversations_with_prefix)



def update_conversation(conversation, schema_name, table_name):
    # Chỉ định các cột cần cập nhật, không bao gồm current_count_message và is_got_oldest_messages
    query = text(f"""
        UPDATE {schema_name}.{table_name}
        SET 
            page_id = :page_id, 
            post_id = :post_id, 
            assign_user_id = :assign_user_id,
            customer_id = :customer_id, 
            customer_fb_id = :customer_fb_id, 
            has_phone = :has_phone, 
            updated_at = :updated_at, 
            message_count = :message_count, 
            tags_id = :tags_id, 
            tag_histories_id = :tag_histories_id, 
            type = :type, 
            phone_number = :phone_number, 
            customer_name = :customer_name,
            tags_text = :tags_text,
            global_id = :global_id,
            ad_ids = :ad_ids
        WHERE id = :id
    """)

    # Loại bỏ các khóa không cần thiết
    filtered_conversation = {
        'page_id': conversation.get('page_id'),
        'post_id': conversation.get('post_id'),
        'assign_user_id': conversation.get('assign_user_id'),
        'customer_id': conversation.get('customer_id'),
        'customer_fb_id': conversation.get('customer_fb_id'),
        'has_phone': conversation.get('has_phone'),
        'updated_at': conversation.get('updated_at'),
        'message_count': conversation.get('message_count'),
        'tags_id': conversation.get('tags_id'),
        'tag_histories_id': conversation.get('tag_histories_id'),
        'type': conversation.get('type'),
        'phone_number': conversation.get('phone_number'),
        'customer_name': conversation.get('customer_name'),
        'id': conversation.get('id'),
        'tags_text': conversation.get('tags_text'),
        'global_id': conversation.get('global_id'),
        'ad_ids' : conversation.get('ad_ids')
    }

    try:
        with engine.connect() as connection:
            transaction = connection.begin()  
            result = connection.execute(query, filtered_conversation)
            transaction.commit()  
            if result.rowcount == 0:
                print("No rows were updated. Please check if the id exists.")
            else:
                print(f"Updated {result.rowcount} row(s).")
    except SQLAlchemyError as e:
        print(f"An error occurred: {e}")


