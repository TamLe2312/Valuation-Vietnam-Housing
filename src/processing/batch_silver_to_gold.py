import os
import sys

# Ép Spark chạy trên IPv4 local
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, log1p, round, when

# Khởi tạo Spark Session Batch
spark = (
    SparkSession.builder
    .appName("SilverToGoldBatchJob")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    print("[*] Đang kiểm tra và đọc dữ liệu từ Silver...")
    df_nhatot = spark.read.parquet("s3a://datalake/silver/unified_real_estate/")

    kaggle_s3_path = "s3a://datalake/silver/kaggle_historical/"
    try:
        df_kaggle = spark.read.parquet(kaggle_s3_path).withColumn("source", lit("kaggle"))
        df_unified = df_kaggle.unionByName(df_nhatot, allowMissingColumns=True)
    except Exception as e:
        print(f"[!] Không đọc được Kaggle từ MinIO ({e}), tiếp tục với chỉ dữ liệu Nhà Tốt...")
        df_unified = df_nhatot

    print(f"[*] Số bản ghi NẠP VÀO BAN ĐẦU: {df_unified.count()}")

    # 1. Feature Engineering (ĐÃ SỬA: Dùng safe_rooms để tránh chia cho 0)
    safe_rooms = when((col("rooms").isNull()) | (col("rooms") == 0), lit(1)).otherwise(col("rooms"))

    df_processed = df_unified \
        .withColumn("area_per_room", round(col("area_m2") / safe_rooms, 2)) \
        .withColumn("toilets_per_room", round(col("toilets") / safe_rooms, 2)) \
        .withColumn("log_price_billion", log1p(col("price_billion"))) \
        .withColumn("log_price_per_m2", log1p(col("price_per_m2_million")))

    # 2. Filter rác và ngoại lai
    df_filtered = df_processed.filter(
        (col("price_billion").isNotNull()) &
        (col("area_m2").isNotNull()) &
        (col("price_billion") >= 0.3) &
        (col("price_billion") <= 500.0) &
        (col("area_m2") >= 10.0) &
        (col("area_m2") <= 2000.0) &
        (col("price_per_m2_million").isNotNull()) &
        (col("price_per_m2_million") >= 2.0) &
        (col("price_per_m2_million") <= 2000.0)
    )
    print(f"[*] Số bản ghi sau khi LỌC ĐIỀU KIỆN (giá, diện tích): {df_filtered.count()}")

    # 3. Loại bỏ trùng lặp (ĐÃ SỬA: Chỉ xóa trùng dựa trên ad_id nếu có)
    if "ad_id" in df_filtered.columns:
        df_cleaned = df_filtered.dropDuplicates(["ad_id"])
        print(f"[*] Số bản ghi sau khi XÓA TRÙNG LẶP THEO ad_id: {df_cleaned.count()}")
    else:
        df_cleaned = df_filtered
        print("[!] Không có cột 'ad_id', bỏ qua bước xóa trùng lặp theo ID tin đăng.")

    # 4. Chọn cột và lưu xuống Gold
    final_cols = [
        "district", "ward", "price_billion",
        "area_m2", "rooms", "toilets", "floors", "is_mat_tien",
        "has_so_hong", "area_per_room", "toilets_per_room",
        "log_price_billion", "log_price_per_m2"
    ]

    selected_cols = [c for c in final_cols if c in df_cleaned.columns]
    df_gold = df_cleaned.select(selected_cols)

    print("[*] Đang ghi xuống Tầng Gold...")
    df_gold.coalesce(1).write \
        .format("parquet") \
        .mode("overwrite") \
        .save("s3a://datalake/gold/master_features/")

    print(f"[OK] Batch Pipeline hoàn tất thành công! Sẵn sàng {df_gold.count()} rows cho Model Train.")

except Exception as err:
    print(f"[ERROR] Lỗi thực thi Pipeline: {err}", file=sys.stderr)
    raise err

finally:
    # Đóng Spark context an toàn để giảm thiểu lỗi file lock trên Windows
    spark.stop()