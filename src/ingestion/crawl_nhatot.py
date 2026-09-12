import random
import requests
import json
import time
from datetime import datetime
from pathlib import Path
import sys

def fetch_nhatot_bronze_data(limit=50):
    max_pages = 5
    if len(sys.argv) > 1:
        try:
            max_pages = int(sys.argv[1])
        except ValueError:
            pass
    # Xác định thư mục data/bronze tại thư mục gốc của project
    project_root = Path(__file__).resolve().parent.parent.parent
    output_dir = project_root / "data" / "bronze"
    output_dir.mkdir(parents=True, exist_ok=True)

    base_url = "https://gateway.chotot.com/v1/public/ad-listing"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nhatot.com/"
    }

    params = {"region_v2": 13000, "cg": 1000, "limit": limit, "o": 0}
    all_listings = []

    print("[*] Bắt đầu cào dữ liệu API Nhà Tốt...")
    for page in range(max_pages):
        params["o"] = page * params["limit"]
        try:
            response = requests.get(base_url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            ads = response.json().get("ads", [])

            if not ads:
                break

            all_listings.extend(ads)
            print(f"  -> Đã lấy trang {page + 1}: {len(ads)} tin.")
            time.sleep(random.uniform(2, 4))
        except requests.exceptions.RequestException as e:
            print(f"[!] Lỗi kết nối trang {page + 1}: {e}")
            break

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"raw_nhatot_{timestamp}.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_listings, f, ensure_ascii=False, indent=2)

    print(f"[OK] Đã lưu {len(all_listings)} tin thô vào: {output_file.name}")


if __name__ == "__main__":
    fetch_nhatot_bronze_data()