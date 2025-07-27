from datetime import datetime, timedelta
import pandas as pd
from crawl_hoithoai_pancake import *
from api import *
from ketnoisql_server import *
import time

# Hàm lấy dữ liệu từ API
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

# Hàm trích xuất từng ad_id và inserted_at
def extract_ads(conversation):
    ads_data = []
    conv_id = conversation.get('id')
    ads = conversation.get('ads', [])
    
    if isinstance(ads, list) and ads:
        for ad in ads:
            ad_id = ad.get('ad_id', '')
            inserted_at = ad.get('inserted_at', '')
            ads_data.append({'id': conv_id, 'ad_id': ad_id, 'inserted_at': inserted_at})
    else:
        ads_data.append({'id': conv_id, 'ad_id': '', 'inserted_at': ''})
    
    return ads_data


def insert_hoithoai_ads(id, ad_id, inserted_at):
    """
    Chèn dữ liệu vào bảng stg.pancake_ads_conversations nếu chưa tồn tại.
    
    :param id: ID của hội thoại
    :param ad_id: ID của quảng cáo
    :param inserted_at: Ngày insert (dạng YYYY-MM-DD)
    """
    check_query = """
        SELECT 1 FROM stg.pancake_ads_conversations
        WHERE id = :id AND ad_id = :ad_id AND inserted_at = :inserted_at
    """
    
    insert_query = """
        INSERT INTO stg.pancake_ads_conversations (id, ad_id, inserted_at) 
        VALUES (:id, :ad_id, :inserted_at)
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(check_query), {"id": id, "ad_id": ad_id, "inserted_at": inserted_at})
        if result.fetchone() is None:  # Không tìm thấy dữ liệu trùng
            conn.execute(text(insert_query), {"id": id, "ad_id": ad_id, "inserted_at": inserted_at})
            conn.commit()
            print(f" Đã chèn: {id} - {ad_id} - {inserted_at}")
        else:
            print(f" Bỏ qua (đã tồn tại): {id} - {ad_id} - {inserted_at}")



import time
import pandas as pd
from datetime import datetime, timedelta


yesterday = datetime.today() - timedelta(days=1)

start_date = datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0)

end_date = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)

since = int(time.mktime(start_date.timetuple()))
until = int(time.mktime(end_date.timetuple()))

# Vòng lặp lấy dữ liệu theo từng khoảng 10 ngày
current_date = start_date
while current_date < end_date:
    # Xác định khoảng thời gian 10 ngày
    next_date = current_date + timedelta(days=20)
    if next_date > end_date:
        next_date = end_date  # Nếu vượt quá ngày hôm qua, chỉ lấy đến ngày hôm qua

    # Chuyển đổi sang timestamp
    since = int(time.mktime(current_date.timetuple()))
    until = int(time.mktime(next_date.timetuple()))

    print(f"Lấy dữ liệu từ {current_date.strftime('%Y-%m-%d')} đến {next_date.strftime('%Y-%m-%d')}")

    # Duyệt qua từng page_id để lấy dữ liệu
    all_conversations_data = []
    for page_id in page_ids:
        access_token = page_access_tokens.get(page_id)
        if access_token:
            conversations = fetch_conversations(page_id, access_token, since, until)
            if conversations:
                for conv in conversations:
                    ads_data = conv.get("ads", [])  # Lấy danh sách ads từ JSON

                    for ad in ads_data:  
                        all_conversations_data.append({
                            'id': conv.get('id', ''),
                            'ad_id': ad.get('ad_id', ''), 
                            'inserted_at': ad.get('inserted_at', '') 
                        })

    # Chuyển dữ liệu thành DataFrame
    df = pd.DataFrame(all_conversations_data, columns=['id', 'ad_id', 'inserted_at'])

    # Chuyển đổi inserted_at sang định dạng YYYY-MM-DD
    df['inserted_at'] = pd.to_datetime(df['inserted_at'], errors='coerce').dt.strftime('%Y-%m-%d')

    # Thêm dữ liệu vào SQL Server nếu chưa tồn tại
    for _, row in df.iterrows():
        insert_hoithoai_ads(row['id'], row['ad_id'], row['inserted_at'])

    print(f"Dữ liệu từ {current_date.strftime('%Y-%m-%d')} đến {next_date.strftime('%Y-%m-%d')} đã được chèn vào SQL Server thành công!")

    # Cập nhật current_date để tiếp tục lấy dữ liệu cho 10 ngày tiếp theo
    current_date = next_date
