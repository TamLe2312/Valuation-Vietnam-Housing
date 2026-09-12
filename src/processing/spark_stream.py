import os
import sys

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["_JAVA_OPTIONS"] = "-Djava.net.preferIPv4Stack=true"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, when, lit, coalesce, round, concat
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import pyspark.sql.functions as F

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("RealEstateStreamingPipeline") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
    .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Schema JSON từ Kafka (bổ sung các field cần thiết)
json_schema = StructType([
    StructField("list_id", StringType(), True),
    StructField("ad_id", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("body", StringType(), True),
    StructField("price", StringType(), True),
    StructField("size", StringType(), True),
    StructField("area_name", StringType(), True),
    StructField("ward_name", StringType(), True),
    StructField("rooms", StringType(), True),
    StructField("toilets", StringType(), True),
    StructField("floors", StringType(), True),
    StructField("house_type", IntegerType(), True),
    StructField("property_legal_document", IntegerType(), True),
    StructField("ingested_at", StringType(), True)
])

# 3. Đọc Stream từ Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "nhatot_raw_stream") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parse JSON & Chuẩn hóa dữ liệu
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), json_schema).alias("data")) \
    .select("data.*")

clean_stream = parsed_stream \
    .withColumn(
    "ad_id",
    concat(
        lit("nhatot_"),
        coalesce(col("list_id"), col("ad_id"), expr("uuid()"))
    )
) \
    .withColumn("source", lit("nhatot")) \
    .withColumn("price_billion", (col("price").cast("float") / 1e9)) \
    .withColumn("area_m2", col("size").cast("float")) \
    .withColumn(
    "price_per_m2_million",
    when(
        col("price_billion").isNotNull() & col("area_m2").isNotNull() & (col("area_m2") > 0),
        round((col("price_billion") * 1000.0) / col("area_m2"), 2)
    ).otherwise(None)
) \
    .withColumn("district", col("area_name")) \
    .withColumn("ward", col("ward_name")) \
    .withColumn("rooms", coalesce(col("rooms").cast(IntegerType()), lit(1))) \
    .withColumn("toilets", coalesce(col("toilets").cast(IntegerType()), lit(1))) \
    .withColumn("floors", coalesce(col("floors").cast(IntegerType()), lit(1))) \
    .withColumn("is_mat_tien", when(col("house_type") == 1, 1).otherwise(0)) \
    .withColumn("has_so_hong", when(col("property_legal_document") == 1, 1).otherwise(0))

filtered_stream = clean_stream.filter(
    col("price_billion").isNotNull() & col("area_m2").isNotNull() &
    (col("price_billion") >= 0.3) & (col("price_billion") <= 250.0) &
    col("district").isNotNull() & (expr("trim(district) != ''"))
).select(
    "ad_id", "source", "district", "ward", "price_billion", "area_m2",
    "rooms", "toilets", "floors",
    "is_mat_tien", "has_so_hong", "ingested_at"
)


# 5. Hàm ghi vào MinIO và Cassandra
def write_to_multiple_sinks(df_batch, batch_id):
    if df_batch.isEmpty():
        return

    print(f"[*] Đang xử lý Micro-Batch ID: {batch_id} với {df_batch.count()} tin mới.")

    # Sink 1: MinIO (Silver Layer)
    df_batch.write \
        .format("parquet") \
        .mode("append") \
        .save("s3a://datalake/silver/unified_real_estate/")

    # Sink 2: Cassandra
    df_batch.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="listings", keyspace="housing") \
        .save()

    stats_df = df_batch.groupBy("district").agg(
        F.round(F.mean("price_billion"), 2).alias("avg_price_billion"),
        F.count("ad_id").alias("ad_count"),
        F.current_timestamp().cast("string").alias("last_updated")
    ).filter(col("district").isNotNull() & (expr("trim(district) != ''")))

    stats_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="district_stats", keyspace="housing") \
        .save()


# 6. Chạy Stream
query = filtered_stream.writeStream \
    .foreachBatch(write_to_multiple_sinks) \
    .outputMode("append") \
    .option("checkpointLocation", "E:/vietnam-housing/checkpoints/nhatot_stream/") \
    .start()

print("[*] 🚀 PySpark Streaming đang lắng nghe Kafka...")
query.awaitTermination()
