#!/usr/bin/env python
import time
from datetime import datetime, timedelta
import json
from decimal import Decimal
import pika
import mysql.connector
from mysql.connector import Error

# Default test host in GTM is 172.16.1.57
# Web UI http://172.16.1.57:15672/#/
RABBITMQ_HOST = '172.16.1.57'
# default port is 5672
RABBITMQ_PORT = 5672

# Default user and password is guest and you can do everything for dev
RABBITMQ_USER = 'guest'
RABBITMQ_PWD  = 'guest'

# gient_dev_sender is only send to Exange giant.direct for permission sample
#RABBITMQ_USER = 'gient_dev_sender'
#RABBITMQ_PWD  = 'gient_dev_sender'

# default virtual host 
RABBITMQ_VIRTUALHOST = '/'
RABBITMQ_EXCHANGE_NAME = 'test'

MYSQL_HOST = '172.16.1.57'
MYSQL_PORT = 3306
MYSQL_DATABASE = 'DBC'
MYSQL_USER = 'dba_user'
MYSQL_PWD = 'testDB@2024'

#exange_name = 'giant.direct'

# 從檔案讀取時間
def read_time_from_file(file_path):
    with open(file_path, 'r') as file:
        return file.read().strip()

# 將時間加 5 分鐘
def add_minutes_to_time(time_str, minutes=10):
    time_obj = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")  # 解析時間字串
    updated_time = time_obj + timedelta(minutes=minutes)  # 加上 5 分鐘
    return updated_time

# 更新時間回檔案
def write_time_to_file(file_path, time_str):
    with open(file_path, 'w') as file:
        file.write(time_str)

# 自定義將 Decimal 和 datetime 轉換為可序列化格式的函數
def custom_default(obj):
    # 處理 Decimal 類型
    if isinstance(obj, Decimal):
        return float(obj)
    # 處理 datetime 類型
    elif isinstance(obj, datetime):
        return obj.isoformat()  # 將 datetime 轉換為 ISO 8601 字符串
    # 如果是 date 類型，也可以用 isoformat() 處理
    elif isinstance(obj, date):
        return obj.isoformat()  # 例如：2024-11-26
    # 如果是其他類型無法序列化，則拋出錯誤
    raise TypeError(f"Type {obj.__class__.__name__} not serializable")

# 處理資料
def process_json_data(json_data):
    data = json.loads(json_data)
    for item in data:
        # 處理每一筆資料，這裡使用 yield 來逐筆返回資料
        yield item

# Send_msg 
def send_msg(exange_name, msg):
    # create connection with login
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PWD)
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_VIRTUALHOST, credentials))
    
    # Create connection by default
    #connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    
    channel = connection.channel()
    channel.basic_publish(exchange= exange_name,
                      routing_key='test_queue1',
                      body= msg)
    print(" [x] Sent " + msg + "\n")


# 建立資料庫連接
def execute_query_with_time_range(start_time, end_time):
    try:
        connection = mysql.connector.connect(
            host = MYSQL_HOST,      # MySQL 資料庫的主機名稱或 IP 地址
            port = MYSQL_PORT,
            database = MYSQL_DATABASE,  # 資料庫名稱
            user = MYSQL_USER,     # 用戶名稱
            password = MYSQL_PWD  # 密碼
        )

        # 檢查連接是否成功
        if connection.is_connected():
            print("成功連接到 MySQL 資料庫")
            
            # 建立一個游標物件
            cursor = connection.cursor()
            
            # 執行一個簡單的查詢
            #cursor.execute("SELECT * FROM DBC.INT_Material_General_Info;")
            
            # SQL 查詢語句，根據從檔案讀取的時間設置時間範圍
            query = """
                    SELECT * FROM DBC.INT_Material_General_Info
                    WHERE ModifiedDateTime BETWEEN %s AND %s limit 10;
                """
            #print(query)
            cursor.execute(query, (start_time, end_time))

            # 獲取查詢結果
            #record = cursor.fetchone()
            #print(f"當前資料庫: {record}")

            # 確保游標已經處理完所有結果
            #cursor.fetchall()  # 如果還有未讀取的結果，這一行會清除它們

            # 取得欄位名稱
            columns = [col[0] for col in cursor.description]  # cursor.description 包含欄位的元資料

            # 讀取所有資料
            rows = cursor.fetchall()

            # 將每一行資料與欄位名稱組合為字典
            result = []
            for row in rows:
                # 使用欄位名稱來組合字典
                row_dict = {columns[i]: row[i] for i in range(len(columns))}
                result.append(row_dict)
            
            # 將資料轉換為 JSON 格式，並處理 Decimal 類型
            json_data = json.dumps(result, default=custom_default, ensure_ascii=False, indent=4)

            # 輸出 JSON 資料
            #print(json_data)

            # 逐筆處理每一筆資料
            for item in process_json_data(json_data):
            # 你可以根據需要處理每一筆資料
                #print(item)
                send_msg(RABBITMQ_EXCHANGE_NAME, json.dumps(item, ensure_ascii=False))


            # 你可以在這裡執行更多的查詢或操作
            # 例如：插入、更新、刪除資料等

    except Error as e:
        print(f"連接錯誤: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL 連接已關閉")


# 主程式
def main():
    file_path = 'sender_timestamp.txt'  # 檔案路徑

    # 從檔案讀取時間
    current_time_str = read_time_from_file(file_path)
    print(f"從檔案讀取的時間: {current_time_str}")

    # 將時間加 5 分鐘
    updated_time = add_minutes_to_time(current_time_str)
    print(f"更新後的時間: {updated_time}")

    # 執行 SQL 查詢，並使用時間範圍作為查詢條件
    execute_query_with_time_range(current_time_str, updated_time)

    # 更新時間回檔案
    #write_time_to_file(file_path, updated_time.strftime("%Y-%m-%d %H:%M:%S"))
    #print(f"更新後的時間已寫回檔案: {updated_time.strftime('%Y-%m-%d %H:%M:%S')}")

# 執行主程式
if __name__ == "__main__":
    main()

#exange_name = 'giant.direct'
#msg = 'Hello Word!!!' +  time.ctime()
#send_msg(exange_name, msg)





