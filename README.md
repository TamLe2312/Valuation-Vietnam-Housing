# 🏡 Valuation Vietnam Housing
> **End-to-End Data Engineering & Machine Learning Platform for Vietnamese Real Estate Valuation**

**Valuation Vietnam Housing** là hệ thống dữ liệu và Machine Learning end-to-end được xây dựng để **thu thập, xử lý, lưu trữ, phân tích dữ liệu bất động sản Việt Nam theo thời gian thực và dự đoán giá nhà**.

Dự án kết hợp:

* 🛰️ Real-time Data Ingestion
* ⚡ Apache Kafka & Spark Streaming
* 🏛️ Medallion Architecture
* 🗄️ MinIO Data Lake
* ⚡ Cassandra NoSQL
* 📊 Grafana Real-time Dashboard
* 🤖 Machine Learning
* 🚀 FastAPI Model Serving
* 🎨 Streamlit Interactive UI
* 🐳 Docker & Docker Compose

---

## 📑 Table of Contents

* [Overview](#-overview)
* [Architecture](#-architecture)
* [Data Flow](#-data-flow)
* [Tech Stack](#-tech-stack)
* [Project Structure](#-project-structure)
* [Data Architecture](#-data-architecture)
* [Machine Learning](#-machine-learning)
* [Getting Started](#-getting-started)
* [Cassandra Setup](#-cassandra-setup)
* [Running the Pipeline](#-running-the-pipeline)
* [Accessing Services](#-accessing-services)
* [Troubleshooting](#-troubleshooting)
* [Future Improvements](#-future-improvements)
* [Author](#-author)

---

# 📖 Overview

Hệ thống được thiết kế theo mô hình **Data Engineering + Machine Learning**, trong đó dữ liệu bất động sản được thu thập liên tục từ nguồn API, đưa vào Kafka và xử lý bằng Spark Streaming.

Dữ liệu sau khi được xử lý sẽ được lưu trữ song song:

1. **MinIO** → Data Lake phục vụ lưu trữ dài hạn và Batch Processing.
2. **Cassandra** → High-performance NoSQL database phục vụ truy vấn real-time.
3. **Grafana** → Trực quan hóa các thống kê thị trường theo quận.

Sau đó, dữ liệu từ Data Lake được xử lý theo batch để tạo dataset sạch ở tầng **Gold**, phục vụ huấn luyện Machine Learning model.

Model được deploy thông qua **FastAPI**, trong khi **Streamlit** cung cấp giao diện để người dùng nhập thông tin bất động sản và nhận kết quả dự đoán.

---

# 🏗 Architecture

```text
                           ┌──────────────────────────┐
                           │     Nhà Tốt / Chợ Tốt    │
                           │          API             │
                           │     Real-time Data       │
                           └────────────┬─────────────┘
                                        │
                                        ▼
                           ┌──────────────────────────┐
                           │      Kafka Producer      │
                           │       Raw JSON Data      │
                           └────────────┬─────────────┘
                                        │
                                        ▼
                           ┌──────────────────────────┐
                           │      Apache Kafka        │
                           │   nhatot_raw_stream      │
                           └────────────┬─────────────┘
                                        │
                                        ▼
                           ┌──────────────────────────┐
                           │    PySpark Streaming     │
                           │                          │
                           │ • Data Cleaning          │
                           │ • Data Type Conversion   │
                           │ • UUID Generation        │
                           │ • Transformation         │
                           └────────────┬─────────────┘
                                        │
                                        │
                                        ▼
┌──────────────────────────┐     ┌──────────────────────────┐
│     Kaggle Dataset       │     │          MinIO           │
│                          │     │        Data Lake         │
│ Historical Housing Data  │────►│                          │
│      Batch Dataset       │     │         BRONZE           │
└──────────────────────────┘     │      Raw / Historical    │
                                 └────────────┬─────────────┘
                                              │
                                              │
                           ┌──────────────────┴──────────────────┐
                           │                                     │
                           ▼                                     ▼
                 ┌───────────────────┐                 ┌────────────────────┐
                 │       MinIO       │                 │     Cassandra      │
                 │    Data Lake      │                 │     NoSQL DB       │
                 │                   │                 │                    │
                 │      SILVER       │                 │ housing.listings   │
                 │     Parquet       │                 │ housing.district_  │
                 │                   │                 │ stats              │
                 └─────────┬─────────┘                 └──────────┬─────────┘
                           │                                      │
                           │                                      ▼
                           │                           ┌────────────────────┐
                           │                           │      Grafana       │
                           │                           │                    │
                           │                           │ Real-time          │
                           │                           │ Dashboard          │
                           │                           └────────────────────┘
                           │
                           ▼
                 ┌────────────────────┐
                 │    Spark Batch     │
                 │                    │
                 │ Silver → Gold      │
                 │ Data Cleaning      │
                 │ Feature Engineering│
                 │ Outlier Handling   │
                 └─────────┬──────────┘
                           │
                           ▼
                 ┌───────────────────┐
                 │   Gold Dataset    │
                 │                   │
                 │    ML-ready       │
                 │     Dataset       │
                 └─────────┬─────────┘
                           │
                           ▼
                 ┌───────────────────┐
                 │  Machine Learning │
                 │                   │
                 │ Ridge Regression  │
                 │ Target Encoding   │
                 └─────────┬─────────┘
                           │
                           ▼
                 ┌───────────────────┐
                 │  Trained Model    │
                 │      .pkl         │
                 └─────────┬─────────┘
                           │
                           ▼
                 ┌───────────────────┐
                 │      FastAPI      │
                 │   Model Serving   │
                 └─────────┬─────────┘
                           │
                           ▼
                 ┌───────────────────┐
                 │     Streamlit     │
                 │   Prediction UI   │
                 └───────────────────┘
```

---

# 🔄 Data Flow

## 1. Data Ingestion

Hệ thống sử dụng **hai nguồn dữ liệu chính**:

### Real-time Source

Dữ liệu bất động sản được thu thập liên tục từ **Nhà Tốt / Chợ Tốt API**.

```text
Nhà Tốt / Chợ Tốt API
        ↓
Kafka Producer
        ↓
Apache Kafka
        ↓
nhatot_raw_stream
```
Dữ liệu được truyền dưới dạng **JSON message** nhằm phù hợp với kiến trúc streaming.

### Historical Source

Hệ thống sử dụng Kaggle Dataset làm nguồn dữ liệu lịch sử để bổ sung dữ liệu cho quá trình phân tích và Machine Learning.
```text
Kaggle Dataset
      ↓
MinIO / Bronze
      ↓
Spark Batch
      ↓
    Silver
```
---

## 2. Real-time Processing

**PySpark Structured Streaming** đọc dữ liệu trực tiếp từ Kafka.

Các bước xử lý chính:

* Parse JSON
* Data validation
* Data cleaning
* Type casting
* Generate unique UUID
* Normalize fields
* Feature preparation
* Timestamp ingestion

Sau đó dữ liệu được ghi vào hai hệ thống:

```text
                    PySpark Streaming
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
            MinIO                   Cassandra
          Data Lake                 Real-time
          Long-term                 Querying
```

---

# 🏛 Medallion Architecture

Dự án sử dụng mô hình **Medallion Architecture**:

```text
                 ┌──────────────┐
                 │    BRONZE    │
                 │              │
                 │ Raw JSON     │
                 │ Kafka Stream │
                 └──────┬───────┘
                        │
                        ▼
                 ┌──────────────┐
                 │    SILVER    │
                 │              │
                 │ Cleaned Data │
                 │ Parquet      │
                 └──────┬───────┘
                        │
                        ▼
                 ┌──────────────┐
                 │     GOLD     │
                 │              │
                 │ ML Ready     │
                 │ Dataset      │
                 └──────┬───────┘
                        │
                        ▼
                 ┌──────────────┐
                 │      ML      │
                 │ Prediction   │
                 └──────────────┘
```

### 🥉 Bronze

Chứa dữ liệu raw từ nguồn API.

Đặc điểm:

* Raw JSON
* Chưa hoặc ít transformation
* Giữ lại dữ liệu gốc
* Phục vụ traceability và reprocessing

### 🥈 Silver

Dữ liệu sau khi được Spark Streaming làm sạch.

Định dạng lưu trữ:

```text
Parquet
```

Các bước xử lý:

* Cleaning
* Type casting
* Deduplication
* UUID generation
* Timestamp normalization

### 🥇 Gold

Dữ liệu đã được xử lý hoàn chỉnh và sẵn sàng cho Machine Learning.

Feature engineering bao gồm:

```text
total_usable_area
area_per_room
price_per_m2
...
```

---

# 🗄 Cassandra

Cassandra được sử dụng như **real-time serving database**.

Keyspace:

```sql
housing
```

### Listings

```sql
CREATE TABLE IF NOT EXISTS listings (
    ad_id text PRIMARY KEY,
    source text,
    district text,
    ward text,
    price_billion float,
    area_m2 float,
    rooms int,
    toilets int,
    floors int,
    is_mat_tien int,
    has_so_hong int,
    ingested_at text
);
```

### District Statistics

Bảng `district_stats` phục vụ dashboard và các truy vấn thống kê nhanh.

```sql
CREATE TABLE IF NOT EXISTS district_stats (
    district text PRIMARY KEY,
    avg_price_billion float,
    ad_count int,
    last_updated text
);
```

Spark Streaming thực hiện **pre-aggregation** trước khi ghi thống kê vào Cassandra nhằm giảm workload cho tầng visualization.

---

# 🤖 Machine Learning

Model được train trên dataset Gold.

## Features

Một số feature chính:

```text
area_m2
rooms
toilets
floors
district
ward
is_mat_tien
has_so_hong
total_usable_area
area_per_room
```

## Algorithms

Dự án sử dụng:

* Ridge Regression
* Target Encoding

Model được serialize bằng:

```text
joblib
```

Model hiện tại:

```text
models/ridge_optimized.pkl
```

---

# 🚀 Model Serving

Machine Learning model được expose thông qua **FastAPI**.

Kiến trúc:

```text
Streamlit
    │
    │ HTTP Request
    ▼
FastAPI
    │
    ▼
Load .pkl Model
    │
    ▼
Prediction
    │
    ▼
JSON Response
```

FastAPI cung cấp Swagger UI để test API:

```text
http://localhost:8000/docs
```

---

# 📊 Real-time Dashboard

**Grafana** kết nối trực tiếp tới Cassandra.

Dashboard tập trung vào các metric như:

* Average property price
* Average price by district
* Market changes
* Real-time listing statistics

Nguồn dữ liệu:

```text
Cassandra
    ↓
housing.district_stats
    ↓
Grafana
```

---

# 🛠 Tech Stack

| Category            | Technology                         |
| ------------------- | ---------------------------------- |
| Language            | Python, SQL, CQL                   |
| Streaming           | Apache Kafka                       |
| Processing          | Apache Spark / PySpark             |
| Data Lake           | MinIO / S3                         |
| NoSQL               | Apache Cassandra                   |
| Data Processing     | Pandas, NumPy                      |
| Machine Learning    | Scikit-learn                       |
| Model               | Ridge Regression |
| Encoding            | Target Encoder                     |
| API                 | FastAPI                            |
| Frontend            | Streamlit                          |
| Visualization       | Grafana                            |
| Containerization    | Docker                             |
| Orchestration       | Docker Compose                     |
| Model Serialization | Joblib                             |

---

# 📂 Project Structure

```text
Valuation-Vietnam-Housing/
│
├── docker-compose.yml
├── requirements.txt
├── README.md
│
├── src/
│   │
│   ├── api/
│   │   ├── main.py
│   │   └── Dockerfile
│   │   └── requirements.txt
│   │
│   ├── app/
│   │   └── main.py
│   │
│   ├── ingestion/
│   │   └── crawl_nhatot.py
│   │   └── kafka_producer.py
│   │
│   ├── models/
│   │   └── train_ridge.py
│   │
│   ├── processing/
│   │   ├── spark_stream.py
│   │   └── batch_silver_to_gold.py
│   │   ├── clean_nhatot.py
│   │   ├── clean_kaggle.py
│
├── models/
│   └── ridge_optimized.pkl
│
└── data/
    └── ...
```

---

# 🚀 Getting Started

## Prerequisites

Cài đặt các công cụ:

* Docker
* Docker Compose
* Python 3.10+
* Java/JDK phù hợp với PySpark
* Git

Clone repository:

```bash
git clone https://github.com/TamLe2312/Valuation-Vietnam-Housing.git

cd Valuation-Vietnam-Housing
```

---

# 🐳 1. Start Infrastructure

Build toàn bộ Docker images:

```bash
docker-compose build --no-cache
```

Khởi động hệ thống:

```bash
docker-compose up -d
```

Kiểm tra containers:

```bash
docker-compose ps
```

Các service chính:

```text
Zookeeper
Kafka
MinIO
Cassandra
Grafana
FastAPI
```

---

# 🗄 2. Cassandra Setup

Truy cập Cassandra container:

```bash
docker exec -it cassandra cqlsh
```

Tạo keyspace:

```sql
CREATE KEYSPACE IF NOT EXISTS housing
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};
```

Chọn keyspace:

```sql
USE housing;
```

Tạo bảng listings:

```sql
CREATE TABLE IF NOT EXISTS listings (
    ad_id text PRIMARY KEY,
    source text,
    district text,
    ward text,
    price_billion float,
    area_m2 float,
    rooms int,
    toilets int,
    floors int,
    is_mat_tien int,
    has_so_hong int,
    ingested_at text
);
```

Tạo bảng thống kê:

```sql
CREATE TABLE IF NOT EXISTS district_stats (
    district text PRIMARY KEY,
    avg_price_billion float,
    ad_count int,
    last_updated text
);
```

---

# ⚡ 3. Run Data Pipeline

## Terminal 1 — Kafka Producer

Chạy data ingestion:

```bash
python src/processing/kafka_producer.py
```

Pipeline:

```text
Nhà Tốt API
     ↓
Kafka Producer
     ↓
nhatot_raw_stream
```

---

## Terminal 2 — Spark Streaming

Chạy Spark Streaming:

```bash
python src/processing/spark_stream.py
```

Pipeline:

```text
Kafka
  ↓
PySpark Streaming
  ├──→ MinIO / Silver
  │
  └──→ Cassandra
```

---

# 🧹 4. Silver → Gold

Sau khi dữ liệu Silver đã được tạo, chạy batch processing:

```bash
python src/processing/batch_silver_to_gold.py
```

Pipeline:

```text
Silver
  ↓
Data Cleaning
  ↓
Outlier Handling
  ↓
Feature Engineering
  ↓
Gold Dataset
```

---

# 🤖 5. Train Machine Learning Model

Chạy training:

```bash
python src/models/train_ridge.py
```

Model sau khi train sẽ được lưu tại:

```text
models/ridge_optimized.pkl
```

---

# 🚀 6. Start FastAPI

FastAPI có thể được chạy thông qua Docker Compose hoặc trực tiếp bằng Uvicorn.

Nếu chạy local:

```bash
uvicorn src.api.main:app --host 0.0.0.0 --port 8000
```

Swagger:

```text
http://localhost:8000/docs
```

---

# 🎨 7. Start Streamlit

Chạy frontend:

```bash
streamlit run src/app/main.py
```

Sau đó truy cập:

```text
http://localhost:8501
```

Người dùng có thể nhập các thông tin như:

```text
District
Ward
Area
Number of rooms
Number of toilets
Number of floors
Facade
Legal status
...
```

và nhận kết quả:

```text
Estimated Property Price
```

---

# 🌐 Access Services

| Service       | URL                          | Purpose             |
| ------------- | ---------------------------- | ------------------- |
| MinIO Console | `http://localhost:9001`      | Data Lake           |
| Grafana       | `http://localhost:3000`      | Real-time Dashboard |
| FastAPI       | `http://localhost:8000`      | ML Backend          |
| FastAPI Docs  | `http://localhost:8000/docs` | API Documentation   |
| Streamlit     | `http://localhost:8501`      | Prediction UI       |
| Cassandra     | `localhost:9042`             | NoSQL Database      |
| Kafka         | `localhost:9092`             | Message Broker      |

### MinIO

Development credentials:

```text
Username: admin
Password: password123
```

> ⚠️ Không sử dụng credentials mặc định này trong production.

---

# 🔧 Troubleshooting

## 1. Scikit-learn Version Mismatch

Nếu gặp lỗi khi load model:

```text
InconsistentVersionWarning
```

hoặc lỗi serialization giữa môi trường train và serve, hãy đảm bảo hai môi trường sử dụng cùng version:

```text
scikit-learn==1.9.0
```

Khuyến nghị:

```text
Training Environment
        │
        ├── Python version
        ├── scikit-learn version
        ├── numpy version
        └── pandas version
                 │
                 ▼
          Serving Environment
```

---

## 2. PySpark Heartbeater Timeout — Windows

Nếu Spark gặp lỗi liên quan tới JVM networking hoặc Heartbeater Timeout, có thể ưu tiên IPv4:

```python
import os

os.environ["_JAVA_OPTIONS"] = (
    "-Djava.net.preferIPv4Stack=true"
)
```

Sau đó khởi động lại Spark application.

---

## 3. Grafana Group By / Value Mapping

Nếu sử dụng plugin `hadesarchitect` và gặp vấn đề khi xử lý chuỗi số liệu:

```text
Transform Name by Regex
```

có thể không cho kết quả mong muốn.

Có thể sử dụng:

```text
Value Mappings
```

kết hợp Regex để xử lý và hiển thị dữ liệu sạch hơn.

---

# 🔐 Security Notes

Đây là project phục vụ mục đích học tập / portfolio.

Trong môi trường production nên thay đổi:

* MinIO credentials
* Cassandra authentication
* Kafka security configuration
* API secrets
* Docker exposed ports
* CORS policy
* Network isolation
* TLS/HTTPS
* Environment variables

Không nên hard-code secrets trực tiếp trong source code.

Khuyến nghị sử dụng:

```text
.env
Docker Secrets
Secret Manager
```
---
# 🎯 Project Goals

Dự án hướng tới việc xây dựng một hệ thống mô phỏng **production-grade real estate data platform**, thay vì chỉ xây dựng một Machine Learning model đơn lẻ.

Các vấn đề chính được giải quyết:

```text
Data Collection
      ↓
Real-time Streaming
      ↓
Data Lake
      ↓
Data Processing
      ↓
Feature Engineering
      ↓
Machine Learning
      ↓
Model Serving
      ↓
Real-time Analytics
      ↓
Visualization
```

Qua đó kết hợp kiến thức của:

* Data Engineering
* Big Data
* Distributed Systems
* Machine Learning
* MLOps
* Backend Development
* Data Visualization
* Docker / DevOps

---

# 👨‍💻 Author

**Tam Le**

*Data Engineer & Machine Learning Developer*

GitHub:

[@TamLe2312](https://github.com/TamLe2312)

---

