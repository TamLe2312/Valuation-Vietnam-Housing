import requests
import json
import time
from datetime import datetime
from kafka import KafkaProducer

# Cấu hình Kafka Broker (Đang trỏ về localhost cho môi trường test trên máy)
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'nhatot_raw_stream'


def create_producer():
    """Khởi tạo Kafka Producer với bộ mã hóa JSON tự động."""
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        # Tự động biến đổi object Python thành chuỗi byte JSON trước khi gửi
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        acks='all',  # Đảm bảo broker đã nhận được dữ liệu thì mới đi tiếp
        retries=3
    )


def stream_nhatot_to_kafka():
    producer = create_producer()

    base_url = "https://gateway.chotot.com/v1/public/ad-listing"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Referer": "https://www.nhatot.com/"
    }
    # Chỉ cần lấy 20 tin mới nhất ở trang đầu tiên
    params = {"region_v2": 13000, "cg": 1000, "limit": 20, "o": 0}

    print(f"[*] 🚀 Bắt đầu Stream dữ liệu vào Kafka Topic: '{TOPIC_NAME}'...")
    print("[*] Nhấn Ctrl+C để dừng.")

    try:
        while True:
            try:
                response = requests.get(base_url, headers=headers, params=params, timeout=10)
                response.raise_for_status()
                ads = response.json().get("ads", [])

                if ads:
                    for ad in ads:
                        # Gắn thêm timestamp để biết thời điểm tin được cào về
                        ad['ingested_at'] = datetime.now().isoformat()

                        # Bắn từng bản ghi vào Kafka
                        producer.send(TOPIC_NAME, value=ad)

                    # Ép buffer đẩy hết dữ liệu đi
                    producer.flush()
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Đã publish {len(ads)} tin mới.")

            except requests.exceptions.RequestException as e:
                print(f"[!] Lỗi API Nhà Tốt: {e}")

            # Cứ mỗi 3 phút (180 giây) thức dậy cào 1 lần để giả lập Real-time
            # Việc nghỉ ngơi giúp bạn không bị nền tảng ban IP
            time.sleep(180)

    except KeyboardInterrupt:
        print("\n[*] Đã dừng luồng Kafka Producer an toàn.")
    finally:
        producer.close()


if __name__ == "__main__":
    stream_nhatot_to_kafka()