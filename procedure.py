import os
import json
import time
from datetime import datetime, date
from confluent_kafka import SerializingProducer
import uuid

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"  )

def get_new_json_files(directory):
    # Lấy danh sách tất cả các file trong thư mục
    files = os.listdir(directory)
    
    # Lọc ra các file JSON chưa được xử lý và bỏ qua 'output.json'
    json_files = [f for f in files if f.endswith('.json') and f != 'output.json']
    
    # Trả về danh sách các file mới
    return sorted(json_files)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    elif isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data["Rank"]),
        value=json.dumps(data, default=json_serializer).encode("utf-8"),
        on_delivery=delivery_report
    )
    producer.flush()

def remove_processed_file(file_path):
    try:
        os.remove(file_path)
        print(f"File {file_path} has been removed after processing.")
    except Exception as e:
        print(f"Error while removing file {file_path}: {e}")

if __name__ == "__main__":
    producer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "error_cb": lambda err: print(f"kafka error: {err}")
    }
    producer = SerializingProducer(producer_config)

    directory = 'E:/DE/final_project/realtime_streaming_coinmarketcap/data'

    while True:
        try:
            # Lấy danh sách file JSON mới
            new_files = get_new_json_files(directory)
            
            if new_files:  # Nếu tìm thấy file mới
                for new_file in new_files:
                    latest_json_file_path = os.path.join(directory, new_file)
                    print(f"Processing new file: {latest_json_file_path}")
                    
                    # Đọc và xử lý file JSON
                    with open(latest_json_file_path, 'r', encoding='utf-8') as file:
                        data_coin = json.load(file)
                    data_full=[]
                    if data_coin:  # Nếu có dữ liệu trong file
                        for data in data_coin:
                            produce_data_to_kafka(producer, "current_coin_prices_topic",data)
                            
                    
                    # Xóa file sau khi xử lý
                    remove_processed_file(latest_json_file_path)
            else:
                print("No new file found. Waiting...")
                time.sleep(2)  # Chờ 1 giây trước khi kiểm tra lại

        except KeyboardInterrupt:
            print('Process terminated by admin (Ctrl+C).')
            break

        except Exception as e:
            print(f"Unexpected error occurred: {e}")
            time.sleep(2)
