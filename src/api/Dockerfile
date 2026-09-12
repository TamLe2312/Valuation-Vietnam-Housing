# Sử dụng Python 3.10 phiên bản rút gọn để tối ưu dung lượng
FROM python:3.10-slim

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Cài đặt thư viện hệ thống cần thiết cho LightGBM
RUN apt-get update && apt-get install -y libgomp1 && rm -rf /var/lib/apt/lists/*

# Copy file requirements và cài đặt thư viện
COPY src/api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy mã nguồn API và thư mục models vào container
COPY src/api/ /app/src/api/
COPY models/ /app/models/

# Mở cổng 8000 để giao tiếp
EXPOSE 8000

# Lệnh khởi động Server bằng Uvicorn
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]