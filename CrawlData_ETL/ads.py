import pyodbc
import pandas as pd
import requests
import time
import json
from datetime import datetime, timedelta
import time

from api import *
from ketnoisql_server import *

# Hàm chuyển đổi ký tự in đậm thành ký tự bình thường
def convert_bold_to_normal(text):
    bold_to_normal = {
        '𝐀': 'A', '𝐁': 'B', '𝐂': 'C', '𝐃': 'D', '𝐄': 'E', '𝐅': 'F', '𝐆': 'G',
        '𝐇': 'H', '𝐈': 'I', '𝐉': 'J', '𝐊': 'K', '𝐋': 'L', '𝐌': 'M', '𝐍': 'N',
        '𝐎': 'O', '𝐏': 'P', '𝐐': 'Q', '𝐑': 'R', '𝐒': 'S', '𝐓': 'T', '𝐔': 'U',
        '𝐕': 'V', '𝐖': 'W', '𝐗': 'X', '𝐘': 'Y', '𝐙': 'Z',
        '𝐚': 'a', '𝐛': 'b', '𝐜': 'c', '𝐝': 'd', '𝐞': 'e', '𝐟': 'f', '𝐠': 'g',
        '𝐡': 'h', '𝐢': 'i', '𝐣': 'j', '𝐤': 'k', '𝐥': 'l', '𝐦': 'm', '𝐧': 'n',
        '𝐨': 'o', '𝐩': 'p', '𝐪': 'q', '𝐫': 'r', '𝐬': 's', '𝐭': 't', '𝐮': 'u',
        '𝐯': 'v', '𝐰': 'w', '𝐱': 'x', '𝐲': 'y', '𝐳': 'z',
        '𝟎': '0', '𝟏': '1', '𝟐': '2', '𝟑': '3', '𝟒': '4', '𝟓': '5', '𝟔': '6',
        '𝟕': '7', '𝟖': '8', '𝟗': '9'
    }
    
    for bold_char, normal_char in bold_to_normal.items():
        text = text.replace(bold_char, normal_char)
    return text

def upsert_data_to_sql(df, connection, table_name, schema_name):
    """Chèn hoặc cập nhật dữ liệu từ DataFrame vào bảng trong SQL Server bằng MERGE."""
    try:
        cursor = connection.cursor()

        # Chuẩn hóa dữ liệu
        df['name'] = df['name'].apply(convert_bold_to_normal)
        df['name'] = df['name'].str.lower()
        df['name'] = df['name'].str.split(' -').str[0]

        for index, row in df.iterrows():
            cursor.execute(f"""
            MERGE {schema_name}.{table_name} AS target
            USING (SELECT ? AS ad_id, ? AS [date]) AS source
            ON target.ad_id = source.ad_id AND target.[date] = source.[date]
            WHEN MATCHED THEN
                UPDATE SET account_id = ?, link_click = ?, 
                    messaging_conversation_started_7d = ?, 
                    messaging_first_reply = ?, post_comments = ?, 
                    status = ?, name = ?, reach = ?, 
                    page_id = ?, budget_remaining = ?, cpc = ?, cpm = ?, 
                    daily_budget = ?, impressions = ?, lifetime_budget = ?, 
                    spend = ?, purchases = ?, ad_status = ?, ctr = ?, 
                    lead_events = ?, purchase_roas = ?, 
                    purchases_conversion_value = ?
            WHEN NOT MATCHED THEN
                INSERT (account_id, ad_id, link_click, messaging_conversation_started_7d, 
                        messaging_first_reply, post_comments, status, name, reach, 
                        page_id, budget_remaining, cpc, cpm, daily_budget, 
                        impressions, lifetime_budget, spend, purchases, ad_status, 
                        ctr, lead_events, purchase_roas, purchases_conversion_value, date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, (
                row['ad_id'], row['date'], row['account_id'], row['link_click'],
                row['messaging_conversation_started_7d'], row['messaging_first_reply'], 
                row['post_comments'], row['status'], row['name'], row['reach'], 
                row['page_id'], row['budget_remaining'], row['cpc'], row['cpm'], 
                row['daily_budget'], row['impressions'], row['lifetime_budget'], 
                row['spend'], row['purchases'], row['ad_status'], row['ctr'], 
                row['lead_events'], row['purchase_roas'], row['purchases_conversion_value'], 
                row['account_id'], row['ad_id'], row['link_click'], 
                row['messaging_conversation_started_7d'], row['messaging_first_reply'], 
                row['post_comments'], row['status'], row['name'], row['reach'], 
                row['page_id'], row['budget_remaining'], row['cpc'], row['cpm'], 
                row['daily_budget'], row['impressions'], row['lifetime_budget'], 
                row['spend'], row['purchases'], row['ad_status'], row['ctr'], 
                row['lead_events'], row['purchase_roas'], row['purchases_conversion_value'], row['date']
            ))

        connection.commit()
    except Exception as e:
        print(f"Lỗi: {e}")
        connection.rollback()
    finally:
        cursor.close()

def connect_to_sql_server(config_file):
    """Kết nối đến SQL Server và trả về đối tượng kết nối."""
    with open(config_file, 'r') as file:
        config = json.load(file)

    db_config = config['connection']

    # Thay thế bằng thông tin kết nối SQL Server của bạn
    server = db_config['server']
    database = db_config['database']
    username = db_config['username']
    password = db_config['password']

    # Tạo chuỗi kết nối
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    # Kết nối đến SQL Server
    try:
        connection = pyodbc.connect(connection_string)
        print("Kết nối thành công!")
    except Exception as e:
        print(f"Lỗi kết nối đến cơ sở dữ liệu: {e}")
        return None  

    return connection  

# Kết nối đến SQL Server
connection = connect_to_sql_server('config.json')

# Hàm để lấy dữ liệu từ API
def get_pancake_data(page_id, access_token, since, until):
    url = f"https://pages.fm/api/public_api/v1/pages/{page_id}/statistics/pages_campaigns"
    params = {
        "page_access_token": access_token,
        "since": since,
        "until": until
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}, message: {response.text}")
        return None

# Lấy ngày hôm nay
today = datetime.today()
   
yesterday = today - timedelta(days=2)

# Đặt thời gian bắt đầu và kết thúc cho ngày hôm qua
start_date = datetime(yesterday.year, yesterday.month, yesterday.day)
end_date = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)

# Lặp qua từng ngày một
current_date = start_date
while current_date <= end_date:
    # Chuyển đổi ngày thành timestamp
    since = int(time.mktime(current_date.timetuple()))
    until = int(time.mktime((current_date + timedelta(days=1)).timetuple()))

    print(f"Đang lấy dữ liệu cho ngày: {current_date.strftime('%Y-%m-%d')}")

    for page_id in page_ids:
        page_access_token = page_access_tokens.get(page_id)
        if not page_access_token:
            print(f"Không có token cho page_id {page_id}")
            continue

        # Lấy dữ liệu cho ngày cụ thể
        data = get_pancake_data(page_id, page_access_token, since, until)
        

        if data:
            df = pd.json_normalize(data, 'data')
            print(df)
            df['date'] = current_date.strftime('%Y-%m-%d') 

            # Gọi hàm upsert để chèn hoặc cập nhật dữ liệu
            upsert_data_to_sql(df, connection, '[Pancake_campaign]', 'stg')

    # Chuyển sang ngày tiếp theo
    current_date += timedelta(days=1)

# Đóng kết nối
connection.close()