from sqlalchemy import text
from ketnoisql_server import engine  
import pandas as pd
from sqlalchemy import text

def execute_insert_hashtag_mapping():
    insert_query = """
    INSERT INTO Hallure_datawarehouse.stg.hashtag_mapping 
    (conversation_id, customer_id, phone_number, inserted_at, Key_hashtag, message, ad_ids)
SELECT
    t.conversation_id,
    t.customer_id,
    kh.phone_number,
    TRY_CAST(t.inserted_at AS DATETIME) AS inserted_at,
    h.Key_hashtag,
    t.messages,
    ht.ad_ids
FROM
    [Hallure_datawarehouse].[stg].[pancake_TinNhan] t
JOIN
    [Hallure_datawarehouse].[stg].[pancake_gd_HoiThoai] ht 
    ON ht.id = t.conversation_id
JOIN
    [Hallure_datawarehouse].[stg].[pancake_dm_KhachHang] kh 
    ON kh.customer_id = t.customer_id
JOIN
    Hallure_datawarehouse.stg.Hashtag h 
    ON t.messages LIKE '%' + h.Key_hashtag + '%'
WHERE
    TRY_CAST(t.inserted_at AS DATETIME) BETWEEN DATEADD(DAY, -10, GETDATE()) AND DATEADD(DAY, -1, GETDATE())
    AND NOT EXISTS (
        SELECT 1
        FROM Hallure_datawarehouse.stg.hashtag_mapping hm
        WHERE 
            hm.conversation_id = t.conversation_id
            AND hm.customer_id = t.customer_id
            AND hm.inserted_at = TRY_CAST(t.inserted_at AS DATETIME)
            AND hm.message = t.messages
    );

    """
    try:
        with engine.connect() as connection:
            with connection.begin():
                connection.execute(text(insert_query))
                print("INSERT query executed successfully!")
    except Exception as e:
        print(f"Error executing INSERT query: {e}")

def insert_hoithoai_mapping():
    insert_query = """
    INSERT INTO [Hallure_datawarehouse].[stg].[hoithoai_mapping] ([conversation_id], [inserted_at])
    SELECT 
        [conversation_id], 
        [inserted_at]
    FROM (
        SELECT 
            [conversation_id], 
            [inserted_at],
            ROW_NUMBER() OVER (PARTITION BY [conversation_id], [inserted_at] ORDER BY [inserted_at]) AS rn
        FROM 
            [Hallure_datawarehouse].[stg].[hashtag_mapping]
        WHERE 
            [conversation_id] IS NOT NULL 
            AND [inserted_at] IS NOT NULL
    ) AS UniqueRecords
    WHERE 
        rn = 1
        AND NOT EXISTS (
            SELECT 1 
            FROM [Hallure_datawarehouse].[stg].[hoithoai_mapping] B 
            WHERE B.conversation_id = UniqueRecords.conversation_id 
            AND B.inserted_at = UniqueRecords.inserted_at
        );
    """
    try:
        with engine.connect() as connection:
            with connection.begin():
                connection.execute(text(insert_query))
                print("INSERT INTO hoithoai_mapping completed successfully!")
    except Exception as e:
        print(f"Error executing INSERT query: {e}")

def update_loai_khach_hang():
    update_query = """
    WITH SplitPhones AS ( 
        SELECT 
            a.conversation_id,
            LTRIM(RTRIM(value)) AS phone_number, 
            a.inserted_at
        FROM 
            [Hallure_datawarehouse].[stg].[hashtag_mapping] a
        CROSS APPLY 
            STRING_SPLIT(a.phone_number, ',')
    ),
    MatchedData AS (
        SELECT DISTINCT 
            a.conversation_id, 
            a.inserted_at
        FROM 
            SplitPhones a
        JOIN 
            [Hallure_dev].[stg].[nhanhvn_dm_KhachHang_api] c
            ON a.phone_number = c.DienThoai
    )
    UPDATE P
    SET 
        P.loai_khach_hang = CASE 
                                WHEN EXISTS (
                                    SELECT 1
                                    FROM MatchedData M
                                    WHERE M.conversation_id = P.conversation_id 
                                      AND M.inserted_at = P.inserted_at
                                ) 
                                THEN 'K1'
                                ELSE 'K0'
                            END
    FROM 
        [Hallure_datawarehouse].[stg].[hoithoai_mapping] P;
    """
    
    try:
        with engine.connect() as connection:
            with connection.begin():
                connection.execute(text(update_query))
                print("Update level_khach_hang completed successfully!")
    except Exception as e:
        print(f"Error executing update query: {e}")

def update_level_khach_hang(engine): 
    update_query = """
        WITH SplitPhones AS (
            SELECT 
                a.conversation_id,
                LTRIM(RTRIM(value)) AS phone_number,
                a.inserted_at
            FROM 
                [Hallure_datawarehouse].[stg].[hashtag_mapping] a
            CROSS APPLY 
                STRING_SPLIT(a.phone_number, ',')
        ),
        MatchedData AS (
            SELECT DISTINCT 
                a.conversation_id,
                a.inserted_at,
                t.Segment
            FROM 
                SplitPhones a
            JOIN 
                [Hallure_datawarehouse].[stg].[Tags_Customers] t
                ON a.phone_number = t.CustomerPhone
        )
        UPDATE P
        SET 
            P.level_khach_hang = M.Segment
        FROM 
            [Hallure_datawarehouse].[stg].[hoithoai_mapping] P
        JOIN 
            MatchedData M 
            ON P.conversation_id = M.conversation_id AND P.inserted_at = M.inserted_at
        WHERE 
            M.Segment IS NOT NULL;
    """
    
    try:
        # Kết nối và thực thi câu lệnh
        with engine.connect() as connection:
            with connection.begin():
                connection.execute(text(update_query))
                print("Update level_khach_hang completed successfully!")
    except Exception as e:
        print(f"Error executing update query: {e}")

def update_level_khach_hang_K0(engine):
    update_query = """
    UPDATE [Hallure_datawarehouse].[stg].[hoithoai_mapping]
    SET level_khach_hang = N'Khách chưa mua hàng'
    WHERE loai_khach_hang = 'K0';
    """
    
    try:
        with engine.connect() as connection:
            with connection.begin():
                connection.execute(text(update_query))
                print("Update level_khach_hang completed successfully!")
    except Exception as e:
        print(f"Error executing update query: {e}")


def def_updated_C3_KN_1(engine):
    try:
        with engine.connect() as conn:
            with conn.begin():  # Start transaction block
                # Update for conversations related to employee care
                query_case_7 = text("""
                UPDATE b
                SET
                    b.Loai_khach_hang = 'K1',
                    b.loai_marketing = 'KN',
                    b.loai_marketing_con = N'KHIẾU NẠI ĐỔI TRẢ',
                    b.loai_hoi_thoai = N'Khiếu nại'
                FROM [Hallure_datawarehouse].[stg].[hoithoai_mapping] b
                JOIN [Hallure_datawarehouse].[stg].[hashtag_mapping] a
                    ON a.conversation_id = b.conversation_id
                    AND a.inserted_at = b.inserted_at
                WHERE 
                    (
                          a.key_hashtag = N'Kiot 3' OR
                            a.key_hashtag = N'Sảnh A2' OR
                            a.key_hashtag = N'378 Minh Khai' OR
                            a.key_hashtag = N'0376748687'      
                    )
                    AND 
                    (b.loai_marketing IS NULL OR b.loai_marketing_con = N'C2 Chưa phân loại');
                """)
                conn.execute(query_case_7)
                print("Update successful.")

    except Exception as e:
        print(f"Transaction failed: {e}")
        raise

def def_updated_C3_KN_3(engine):
    try:
        # Kết nối đến cơ sở dữ liệu
        with engine.connect() as conn:
            print("Database connection established.")
            # Bắt đầu giao dịch
            with conn.begin() as transaction:
                print("Transaction started.")
                
                # Câu truy vấn SQL
                query_case_7 = text("""
                UPDATE b
                SET 
                    b.Loai_khach_hang = 'K1',
                    b.loai_marketing = 'C3',
                    b.loai_marketing_con = N'C3 KHÁCH MUA HÀNG',
                    b.loai_hoi_thoai = N'Mua Hàng'
                FROM 
                    [Hallure_datawarehouse].[stg].[hoithoai_mapping] b  
                JOIN 
                    [Hallure_datawarehouse].[stg].[hashtag_mapping] a  
                    ON a.conversation_id = b.conversation_id 
                    AND a.inserted_at = b.inserted_at
                WHERE 
                    a.key_hashtag IN (
                        N'gửi thông tin đơn hàng', 
                        N'TK Nguyễn Anh Tuấn',
                        N'8851342707',
                        N'Em gửi chị thông tin STK duy nhất bên em',
                        N'chị nhận không ưng mình có thể đổi mẫu hoặc size',       
                        N'xác nhận đơn', 
                        N'gửi chị thông tin đơn hàng',
                        N'thông tin đơn', 
                        N'gửi chị đơn',
                        N'KHMOI',
                        N'Bên em chưa áp dụng kiểm tra hàng trước',
                        N'kèm chính sách đổi và hướng dẫn sử dụng bảo quản sản phẩm',
                        N'hướng dẫn đổi trả',
                        N'phiếu đặt hàng', 
                        N'gửi lại đơn',
                        N'Chuyển thành công chị cho em xin ảnh giao dịch nhé ạ',
                        N'thu cod',  
                        N'nhận chuyển khoản trước'     
                    )
                    AND (b.loai_marketing IS NULL OR b.loai_marketing_con = N'C2 Chưa phân loại');
                """)
                
                # Thực thi truy vấn
                result = conn.execute(query_case_7)
                print(f"Update successful, {result.rowcount} rows affected.")
    except Exception as e:
        print(f"An error occurred: {e}")



def process_conversations_case_5(engine):
    # Lấy dữ liệu từ bảng hashtag_mapping
    with engine.connect() as conn:
        query = """
        SELECT 
            [id],
            a.conversation_id,
            [customer_id],
            a.inserted_at,
            [Key_hashtag],
            [phone_number],
            [message]
        FROM [Hallure_datawarehouse].[stg].[hashtag_mapping] a
        join [Hallure_datawarehouse].[stg].[hoithoai_mapping] b
        ON a.conversation_id = b.conversation_id 
                AND a.inserted_at = b.inserted_at
        Where b.loai_marketing = N'C2' and b.loai_marketing_con =N'C2 Chưa phân loại'
        """
        df = pd.read_sql(query, conn)

    # Danh sách từ khóa hỏi giá
    keywords = set(['xin giá', 'bn', 'bao nhieu', 'Xin giá', 'giá', 'báo giá'])

    # Khởi tạo danh sách để lưu kết quả
    updates = []

    # Tách các tin nhắn
    for conversation_id, group in df.groupby('conversation_id'):
        messages = group['message'].iloc[0].split('||')  # Giả sử mỗi cuộc hội thoại có 1 tin nhắn duy nhất
        messages = [msg.strip() for msg in messages if msg.strip()]

        for i in range(len(messages)):
            message = messages[i]

            # Kiểm tra xem tin nhắn có chứa từ khóa hỏi giá hay không
            if any(keyword in message for keyword in keywords):
                # Kiểm tra tin nhắn tiếp theo
                if i + 1 < len(messages) and messages[i + 1].startswith('NV:'):
                    # Kiểm tra xem KH có gửi tin nhắn tiếp theo không
                    if i + 2 < len(messages) and messages[i + 2].startswith('KH:'):
                        continue  # KH đã trả lời
                    else:
                        # KH không trả lời, chuẩn bị cập nhật
                        updates.append({
                            'conversation_id': conversation_id,
                            'inserted_at': group.iloc[0]['inserted_at'],  # Giữ nguyên thời gian gửi
                            'loai_marketing': "C2",
                            'loai_hoi_thoai': "Mua hàng",
                            'loai_marketing_con': "C2D Hỏi giá xong im lặng",
                        })

    # In danh sách các bản ghi chuẩn bị cập nhật
    print("Updates to be made:", updates)

    # Nếu cần, cập nhật vào bảng hoithoai_mapping
    if updates:
        with engine.connect() as conn:  # Mở lại kết nối
            trans = conn.begin()  # Bắt đầu transaction
            try:
                update_query = text("""
                    UPDATE [Hallure_datawarehouse].[stg].[hoithoai_mapping]
                    SET loai_marketing_con = :loai_marketing_con
                    WHERE conversation_id = :conversation_id 
                    AND inserted_at = :inserted_at 
                    AND (loai_marketing = N'C2' and loai_marketing_con =N'C2 Chưa phân loại')
                """)
                
                # Tạo một danh sách các tham số
                update_params = [{
                    'loai_marketing_con': update['loai_marketing_con'],
                    'conversation_id': update['conversation_id'],
                    'inserted_at': update['inserted_at']
                } for update in updates]

                # Sử dụng executemany để cập nhật nhiều bản ghi cùng lúc
                conn.execute(update_query, update_params)

                trans.commit()  # Commit transaction nếu không có lỗi
            except Exception as e:
                trans.rollback()  # Rollback nếu có lỗi
                print(f"Transaction failed: {e}")

def process_conversations_case_6(engine):
    # Lấy dữ liệu từ bảng hashtag_mapping
    with engine.connect() as conn:
        query = """
        SELECT 
            [id],
            a.conversation_id,
            [customer_id],
            a.inserted_at,
            [Key_hashtag],
            [phone_number],
            [message]
        FROM [Hallure_datawarehouse].[stg].[hashtag_mapping] a
        JOIN [Hallure_datawarehouse].[stg].[hoithoai_mapping] b
        ON a.conversation_id = b.conversation_id 
            AND a.inserted_at = b.inserted_at
        WHERE b.loai_marketing = N'C2' and b.loai_marketing_con =N'C2 Chưa phân loại'
        """
        df = pd.read_sql(query, conn)

    # Khởi tạo danh sách để lưu kết quả
    updates = []

    # Tách các tin nhắn
    for conversation_id, group in df.groupby('conversation_id'):
        # Tách tin nhắn thành danh sách
        messages = group['message'].iloc[0].split('||')  # Giả sử mỗi cuộc hội thoại có 1 tin nhắn duy nhất

        # Loại bỏ khoảng trắng và ký tự thừa
        messages = [msg.strip() for msg in messages if msg.strip()]

        # Kiểm tra nếu tất cả tin nhắn chỉ do KH gửi và không có NV phản hồi
        only_kh_messages = all(msg.startswith('KH:') for msg in messages) and not any(msg.startswith('NV:') for msg in messages)
       
        if only_kh_messages:
            # Nếu chỉ có KH nhắn tin hoặc KH là người nhắn cuối cùng
            updates.append({
                'conversation_id': conversation_id,
                'inserted_at': group.iloc[0]['inserted_at'],  # Giữ nguyên thời gian gửi
                'loai_marketing_con': "C2E CHĂM SÓC CHƯA TỐT",
            })

    # In danh sách các bản ghi chuẩn bị cập nhật
    print("Updates to be made:", updates)

    # Nếu cần, cập nhật vào bảng hoithoai_mapping
    if updates:
        with engine.connect() as conn:  # Mở lại kết nối
            trans = conn.begin()  # Bắt đầu transaction
            try:
                update_query = text("""
                    UPDATE [Hallure_datawarehouse].[stg].[hoithoai_mapping]
                    SET loai_marketing_con = :loai_marketing_con
                    WHERE conversation_id = :conversation_id 
                    AND inserted_at = :inserted_at 
                    AND (loai_marketing_con =N'C2 Chưa phân loại')
                """)
                
                # Tạo danh sách tham số từ updates
                update_params = [{
                    'loai_marketing_con': update['loai_marketing_con'],
                    'conversation_id': update['conversation_id'],
                    'inserted_at': update['inserted_at']
                } for update in updates]

                # Sử dụng executemany để thực thi nhiều bản ghi
                conn.execute(update_query, update_params)

                trans.commit()  # Commit transaction nếu không có lỗi
            except Exception as e:
                trans.rollback()  # Rollback nếu có lỗi
                print(f"Transaction failed: {e}")

def def_updated_C2(engine):
    try:
        # Begin transaction
        with engine.connect() as conn:
            conn.begin()  # Start the transaction

             # Case 8 - Update for records without any marketing classification
            query_case_8 = text("""
            UPDATE [Hallure_datawarehouse].[stg].[hoithoai_mapping]
            SET loai_marketing = N'C2',
                loai_marketing_con = N'C2 chưa phân loại',
                loai_hoi_thoai = N'Mua hàng'
            WHERE loai_marketing IS NULL ;
            """)
            conn.execute(query_case_8)
            
            # Case 1 - Update for 'COMMENT' type conversations
            query_case_1 = text("""
            UPDATE b
            SET 
   
                b.loai_marketing_con = N'C2F KHÁCH COMMENT BÀI VIẾT'
            FROM 
                [Hallure_datawarehouse].[stg].[hoithoai_mapping] b  
            JOIN 
                [Hallure_datawarehouse].[stg].[hashtag_mapping] a  
                ON a.conversation_id = b.conversation_id 
                AND a.inserted_at = b.inserted_at
            JOIN 
                [Hallure_datawarehouse].[stg].pancake_gd_HoiThoai c
                ON b.conversation_id = c.id 
                AND b.inserted_at = c.updated_at
            WHERE 
                (c.type = 'COMMENT' OR a.message LIKE N'%Hallure inbox cho chị rồi đó ạ%' )
                and loai_marketing = N'C2';
            """)
            conn.execute(query_case_1)

            # Case 2 - Update for specific messages about high prices
            query_case_2 = text("""
            UPDATE a
            SET a.loai_marketing_con = N'C2A GIÁ CAO'
            FROM [Hallure_datawarehouse].[stg].[hoithoai_mapping] a
            JOIN [Hallure_datawarehouse].[stg].[hashtag_mapping] b 
                ON a.conversation_id = b.conversation_id 
                AND a.inserted_at = b.inserted_at
            WHERE 
                (
                    b.message LIKE N'%Giá cao%' 
                    OR b.message LIKE N'%giá cao%' 
                    OR b.message LIKE N'%giá không hợp túi tiền%' 
                    OR b.message LIKE N'%gia cao%' 
                    OR b.message LIKE N'%Gia cao%'
                )
                AND a.loai_marketing_con =N'C2 Chưa phân loại';
            """)
            conn.execute(query_case_2)

            # Case 3 - Update for messages about "sold out" or "out of stock"
            query_case_3 = text("""
           UPDATE a
            SET a.loai_marketing_con = N'C2B HẾT HÀNG HẾT SIZE'
            FROM [Hallure_datawarehouse].[stg].[hoithoai_mapping] a
            JOIN [Hallure_datawarehouse].[stg].[hashtag_mapping] b 
                ON a.conversation_id = b.conversation_id 
                AND a.inserted_at = b.inserted_at
            WHERE( b.Key_hashtag IN (
                N'hết hàng', N'sold out', N'hết size'
            ) or b.message like N'%hết sz%')
            AND a.loai_marketing_con =N'C2 Chưa phân loại';
            """)
            conn.execute(query_case_3)

            # Case 4 - Update for messages about "not suitable" or "don't like"
            query_case_4 = text("""
            UPDATE a
            SET a.loai_marketing_con = N'C2C KHÔNG HỢP NHU CẦU'
            FROM [Hallure_datawarehouse].[stg].[hoithoai_mapping] a 
            JOIN [Hallure_datawarehouse].[stg].[hashtag_mapping] b 
                ON a.conversation_id = b.conversation_id 
                AND a.inserted_at = b.inserted_at
            WHERE 
                (
                    b.message LIKE N'%Không hợp%' 
                    OR b.message LIKE N'%Không thích%' 
                    OR b.message LIKE N'%không thích%' 
                    OR b.message LIKE N'%không hợp%' 
                    OR b.message LIKE N'%kh hop%' 
                    OR b.message LIKE N'%kh thich%' 
                    OR b.message LIKE N'%khong hop%' 
                    OR b.message LIKE N'%khong thich%'
                )
                AND (a.loai_marketing = N'C2' and a.loai_marketing_con =N'C2 Chưa phân loại');
            """)
            conn.execute(query_case_4)

            # Commit the transaction
            conn.commit()

    except Exception as e:
        # If any exception occurs, rollback the transaction
        conn.rollback()
        print(f"Transaction failed: {e}")
        raise

def def_updated_C2_v2(engine):
    try:
        # Begin transaction
        with engine.connect() as conn:
            conn.begin()  # Start the transaction
            

            # Case 7 - Update for conversations related to employee care
            query_case_7 = text("""
            UPDATE a
            SET a.loai_marketing_con = N'C2 NHÂN VIÊN ĐANG CHĂM SÓC'
            FROM [Hallure_datawarehouse].[stg].[hoithoai_mapping] a
            JOIN [Hallure_datawarehouse].[stg].[hashtag_mapping] b 
                ON a.conversation_id = b.conversation_id 
                AND a.inserted_at = b.inserted_at
             WHERE 
                (
                    b.message LIKE N'%tư vấn%' 
                    OR b.message LIKE N'%-<3-%'
					or b.message LIKE N'sản phẩm mới hôm nay' 
					or b.message LIKE N'đã trả lời một quảng cáo' 
					or b.message LIKE N'em chào chị, chị đang quan tâm đến mẫu' 
					or b.message LIKE N'Chúng tôi có thể giúp gì cho bạn' 
                )
                AND (a.loai_marketing_con =N'C2 Chưa phân loại');
            """)
            conn.execute(query_case_7)

            # Commit the transaction
            conn.commit()

    except Exception as e:
        # If any exception occurs, rollback the transaction
        conn.rollback()
        print(f"Transaction failed: {e}")
        raise

def def_updated_C3_KN_2(engine):
    try:
        # Begin transaction
        with engine.connect() as conn:
            conn.begin()  # Start the transaction
            

            # Case 7 - Update for conversations related to employee care
            query_case_7 = text("""
            DECLARE @StartDate DATE, @EndDate DATE, @CurrentDate DATE;

            -- Lấy giá trị StartDate và EndDate từ subquery hoặc theo cách khác
            SELECT 
                @StartDate = MIN(inserted_at),
                @EndDate = MAX(inserted_at)
            FROM [Hallure_datawarehouse].[stg].[hoithoai_mapping] where loai_marketing is null;

            -- Khởi tạo CurrentDate với StartDate
            SET @CurrentDate = @StartDate;

            -- Vòng lặp qua từng ngày
            WHILE @CurrentDate <= @EndDate
            BEGIN
                -- Truy vấn dữ liệu cho từng ngày
               WITH SplitPhones AS (
				SELECT a.customer_id, a.name,
					   LTRIM(RTRIM(value)) AS phone_number, 
					   a.inserted_at
				FROM 
					[Hallure_datawarehouse].[stg].[pancake_dm_KhachHang] a
				CROSS APPLY 
					STRING_SPLIT(a.phone_number, ',')
			)
			, FilteredData AS (
				SELECT DISTINCT 
					e.conversation_id, 
					e.inserted_at,  
					c.OrderDate,
					e.messages,
					a.name,
					c.CustomerName
				FROM 
					SplitPhones a
				JOIN 
					[Hallure].[oms].[C_Order] c
					ON a.phone_number = c.CustomerPhone
				JOIN 
					[Hallure_datawarehouse].[stg].[pancake_TinNhan] e
					ON a.customer_id = e.customer_id
				JOIN 
					[Hallure_datawarehouse].[stg].[pancake_gd_HoiThoai] k
					ON a.customer_id = k.customer_id
					    WHERE 
							CONVERT(DATE, e.inserted_at) =  @CurrentDate
							AND CONVERT(DATE, c.OrderDate) BETWEEN CONVERT(DATE,  @CurrentDate)
							AND DATEADD(DAY, 0, CONVERT(DATE, e.inserted_at)) 
							AND k.type = 'INBOX'
					)
                
					UPDATE p
                        SET
                            p.loai_marketing = 'C3',
                            p.loai_marketing_con = N'C3 KHÁCH MUA HÀNG',
                            p.loai_hoi_thoai = N'Mua Hàng'  from
					FilteredData v join stg.hoithoai_mapping p on p.conversation_id = v.conversation_id and p.inserted_at = v.inserted_at where p.loai_marketing is null
                        -- Tăng ngày hiện tại lên 1
                        SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
                    ENd
            """)
            conn.execute(query_case_7)

            # Commit the transaction
            conn.commit()

    except Exception as e:
        # If any exception occurs, rollback the transaction
        conn.rollback()
        print(f"Transaction failed: {e}")
        raise