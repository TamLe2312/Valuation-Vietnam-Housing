import json
import re
from pathlib import Path
import polars as pl

DISTRICT_MAPPING = {
    "Quận 1": "Quận 1", "Quan 1": "Quận 1", "Q1": "Quận 1",
    "Quận 2": "Thành phố Thủ Đức", "Quan 2": "Thành phố Thủ Đức", "Q2": "Thành phố Thủ Đức",
    "Quận 9": "Thành phố Thủ Đức", "Quan 9": "Thành phố Thủ Đức", "Q9": "Thành phố Thủ Đức",
    "Thủ Đức": "Thành phố Thủ Đức", "Quận Thủ Đức": "Thành phố Thủ Đức", "Tp Thủ Đức": "Thành phố Thủ Đức",
    "Quận 3": "Quận 3", "Quan 3": "Quận 3", "Q3": "Quận 3",
    "Quận 4": "Quận 4", "Quan 4": "Quận 4", "Q4": "Quận 4",
    "Quận 5": "Quận 5", "Quan 5": "Quận 5", "Q5": "Quận 5",
    "Quận 6": "Quận 6", "Quan 6": "Quận 6", "Q6": "Quận 6",
    "Quận 7": "Quận 7", "Quan 7": "Quận 7", "Q7": "Quận 7",
    "Quận 8": "Quận 8", "Quan 8": "Quận 8", "Q8": "Quận 8",
    "Quận 10": "Quận 10", "Quan 10": "Quận 10", "Q10": "Quận 10",
    "Quận 11": "Quận 11", "Quan 11": "Quận 11", "Q11": "Quận 11",
    "Quận 12": "Quận 12", "Quan 12": "Quận 12", "Q12": "Quận 12",
    "Bình Thạnh": "Quận Bình Thạnh", "Quận Bình Thạnh": "Quận Bình Thạnh",
    "Gò Vấp": "Quận Gò Vấp", "Quận Gò Vấp": "Quận Gò Vấp",
    "Phú Nhuận": "Quận Phú Nhuận", "Quận Phú Nhuận": "Quận Phú Nhuận",
    "Tân Bình": "Quận Tân Bình", "Quận Tân Bình": "Quận Tân Bình",
    "Tân Phú": "Quận Tân Phú", "Quận Tân Phú": "Quận Tân Phú",
    "Bình Tân": "Quận Bình Tân", "Quận Bình Tân": "Quận Bình Tân",
    "Nhà Bè": "Huyện Nhà Bè", "Huyện Nhà Bè": "Huyện Nhà Bè",
    "Hóc Môn": "Huyện Hóc Môn", "Huyện Hóc Môn": "Huyện Hóc Môn",
    "Bình Chánh": "Huyện Bình Chánh", "Huyện Bình Chánh": "Huyện Bình Chánh",
    "Củ Chi": "Huyện Củ Chi", "Huyện Củ Chi": "Huyện Củ Chi",
    "Cần Giờ": "Huyện Cần Giờ", "Huyện Cần Giờ": "Huyện Cần Giờ"
}



def clean_text_location(text: str) -> str:
    if not text: return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def process_silver_nhatot():
    project_root = Path(__file__).resolve().parent.parent.parent
    bronze_dir = project_root / "data" / "bronze"
    silver_dir = project_root / "data" / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    json_files = list(bronze_dir.glob("raw_nhatot_*.json"))
    if not json_files:
        print("[!] Không có file JSON nào để xử lý.")
        return

    extracted_records = []
    for fpath in json_files:
        with open(fpath, "r", encoding="utf-8") as f:
            for item in json.load(f):
                extracted_records.append({
                    "ad_id": f"nhatot_{item.get('list_id', '')}",
                    "source": "nhatot",
                    "title": item.get("subject", ""),
                    "description": item.get("body", ""),
                    "price_raw": item.get("price"),
                    "area_raw": item.get("size") or item.get("area"),
                    "district_raw": item.get("area_name", ""),
                    "ward_raw": item.get("ward_name", ""),
                    "rooms": item.get("rooms"),
                    "toilets": item.get("toilets"),
                    "floors": item.get("floors"),
                })

    df = pl.DataFrame(extracted_records).unique(subset=["ad_id"])

    df = df.with_columns([
        pl.col("price_raw").cast(pl.Float64, strict=False),
        pl.col("area_raw").cast(pl.Float64, strict=False),
        pl.col("rooms").cast(pl.Int32, strict=False),
        pl.col("toilets").cast(pl.Int32, strict=False),
        pl.col("floors").cast(pl.Int32, strict=False),
        pl.col("district_raw").map_elements(clean_text_location, return_dtype=pl.Utf8).replace(DISTRICT_MAPPING).alias(
            "district"),
        pl.col("ward_raw").map_elements(clean_text_location, return_dtype=pl.Utf8).alias("ward")
    ])

    df = df.with_columns([
        (pl.col("price_raw") / 1e9).round(4).alias("price_billion"),
        pl.col("area_raw").alias("area_m2")
    ])

    df = df.with_columns(
        ((pl.col("price_billion") * 1000) / pl.col("area_m2")).round(2).alias("price_per_m2_million")
    )

    # Lọc outlier cơ bản
    df_clean = df.filter(
        pl.col("price_billion").is_not_null() & pl.col("area_m2").is_not_null() &
        (pl.col("price_billion") >= 0.3) & (pl.col("price_billion") <= 250.0) &
        (pl.col("area_m2") >= 8.0) & (pl.col("area_m2") <= 2000.0) &
        pl.col("district").is_not_null()
    )

    # Extract Regex từ mô tả
    df_clean = df_clean.with_columns([
        pl.col("description").str.to_lowercase().str.contains("mặt tiền").cast(pl.Int32).alias("is_mat_tien"),
        (pl.col("description").str.to_lowercase().str.contains("sổ hồng") |
         pl.col("description").str.to_lowercase().str.contains("sổ đỏ")).cast(pl.Int32).alias("has_so_hong")
    ])

    df_clean = df_clean.with_columns(
        (pl.col("area_m2") // 20 * 20).alias("area_bin")
    )

    df_clean = df_clean.with_columns([
        pl.col("rooms").fill_null(pl.median("rooms").over(["district", "area_bin"])),
        pl.col("toilets").fill_null(pl.median("toilets").over(["district", "area_bin"])),
        pl.col("floors").fill_null(pl.median("floors").over(["district", "area_bin"]))
    ])
    # Nếu vẫn còn NA (do cả 1 quận/nhóm diện tích đó không ai nhập), điền mặc định
    df_clean = df_clean.with_columns([
        pl.col("rooms").fill_null(2).cast(pl.Int32),
        pl.col("toilets").fill_null(2).cast(pl.Int32),
        pl.col("floors").fill_null(1).cast(pl.Int32)
    ]).drop("area_bin")
    # Lọc bỏ dòng lỗi diện tích / giá thiếu hoặc không hợp lệ

    final_cols = ["ad_id", "source", "district", "ward", "price_billion", "area_m2",
                  "price_per_m2_million", "rooms", "toilets", "floors", "is_mat_tien", "has_so_hong"]

    output_file = silver_dir / "silver_nhatot.parquet"
    df_clean.select(final_cols).write_parquet(output_file, compression="snappy")
    print(f"[OK] Đã chuẩn hóa {len(df_clean)} tin Nhà Tốt vào {output_file.name}")


if __name__ == "__main__":
    process_silver_nhatot()