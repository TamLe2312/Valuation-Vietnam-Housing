import subprocess
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

def run_step(command, step_name):
    logging.info(f"Bắt đầu: {step_name}")
    res = subprocess.run([sys.executable] + command.split(), capture_output=False)
    if res.returncode != 0:
        logging.error(f"{step_name} thất bại với mã lỗi {res.returncode}")
        sys.exit(res.returncode)
    logging.info(f"Hoàn thành: {step_name}")

if __name__ == "__main__":
    # Bước 1: Tổng hợp dữ liệu lên tầng Gold
    run_step("processing/batch_silver_to_gold.py", "Batch Gold Feature Pipeline")

    # Bước 2: Huấn luyện lại mô hình
    run_step("models/train_ridge.py", "Retrain Ridge Regression Pipeline")

    logging.info("[SUCCESS] Toàn bộ quy trình định kỳ đã hoàn tất!")