import re
import pandas as pd
import polars as pl
from pathlib import Path

# Bảng ánh xạ đồng bộ tên Quận/Huyện với Tầng Silver của Nhà Tốt
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


# def repair_vietnamese_mojibake(text):
#     """Sửa lỗi mã hóa tiếng Việt UTF-8 bị đọc nhầm thành Windows-1252 / Latin-1."""
#     if not isinstance(text, str):
#         return text
#     try:
#         return text.encode('cp1252').decode('utf-8')
#     except Exception:
#         pass
#     try:
#         return text.encode('latin1').decode('utf-8')
#     except Exception:
#         pass
#     res = []
#     for c in text:
#         try:
#             res.append(c.encode('cp1252'))
#         except Exception:
#             try:
#                 res.append(c.encode('latin1'))
#             except Exception:
#                 res.append(c.encode('utf-8', errors='ignore'))
#     try:
#         return b"".join(res).decode('utf-8')
#     except Exception:
#         return text
#

def _clean_number(num_str: str) -> float:
    """
    Chuẩn hóa chuỗi số về float.
    Xử lý cả 1.5, 1,5, 1.200.000, 1,200,000, 1200000.
    """
    num_str = num_str.strip()

    # Đếm số lượng dấu chấm và phẩy
    dot_count = num_str.count(".")
    comma_count = num_str.count(",")

    # 1. Nếu có cả chấm và phẩy: dấu nào ở cuối cùng là dấu thập phân
    if dot_count > 0 and comma_count > 0:
        last_dot = num_str.rfind(".")
        last_comma = num_str.rfind(",")
        if last_dot > last_comma:
            # Dấu chấm là thập phân: 1,200.5 -> bỏ phẩy
            num_str = num_str.replace(",", "")
        else:
            # Dấu phẩy là thập phân: 1.200,5 -> bỏ chấm, đổi phẩy thành chấm
            num_str = num_str.replace(".", "").replace(",", ".")

    # 2. Nếu chỉ có nhiều hơn 1 dấu chấm (hoặc phẩy) -> chắc chắn là phân cách hàng nghìn
    elif dot_count > 1:
        num_str = num_str.replace(".", "")
    elif comma_count > 1:
        num_str = num_str.replace(",", "")

    # 3. Nếu chỉ có đúng 1 dấu chấm hoặc 1 dấu phẩy:
    elif dot_count == 1 or comma_count == 1:
        sep = "." if dot_count == 1 else ","
        parts = num_str.split(sep)
        # Nếu phần đuôi có đúng 3 chữ số (ví dụ 1.200 hoặc 850,000) -> phân cách hàng nghìn
        if len(parts[1]) == 3 and len(parts[0]) <= 3:
            num_str = "".join(parts)
        else:
            # Còn lại là số thập phân (1.5 hoặc 2,5)
            num_str = parts[0] + "." + parts[1]

    return float(num_str)


def parse_price(val):
    """Quy đổi chuỗi giá tiếng Việt về số thực (Tỷ VNĐ)."""
    if pd.isna(val) or not isinstance(val, str):
        return None

    text = val.lower().strip()

    if any(k in text for k in ["thỏa thuận", "thoa thuan", "liên hệ", "lien he", "tùy", "thuê"]):
        if not re.search(r"\d", text):
            return None

    text = text.replace("tỉ", "tỷ").replace("rưỡi", ".5")

    short_match = re.search(r"^(\d+)\s*t\s*(\d+)$", text)
    if short_match:
        ty_part = int(short_match.group(1))
        tr_part = int(short_match.group(2))
        # Nếu 2t5 nghĩa là 2 tỷ 500 triệu (nhân lên tương ứng chữ số)
        multiplier = 100 if len(str(tr_part)) == 1 else 1
        return round(ty_part + (tr_part * multiplier) / 1000.0, 4)

    total_ty = 0.0
    matched = False
    match_ty = re.search(r"([\d\.,]+)\s*(?:tỷ|ty)", text)
    if match_ty:
        try:
            total_ty += _clean_number(match_ty.group(1))
            matched = True
        except ValueError:
            pass

    # 2. Tìm phần TRIỆU: (ví dụ: "500 triệu", "1.200 tr", "300tr")
    match_tr = re.search(r"([\d\.,]+)\s*(?:triệu|trieu|tr)", text)
    if match_tr:
        try:
            total_ty += _clean_number(match_tr.group(1)) / 1000.0
            matched = True
        except ValueError:
            pass

    # 3. Tìm phần NGHÌN / K: (ví dụ: "500k", "500 nghìn")
    match_k = re.search(r"([\d\.,]+)\s*(?:nghìn|ngan|k)", text)
    if match_k:
        try:
            total_ty += _clean_number(match_k.group(1)) / 1000000.0
            matched = True
        except ValueError:
            pass

    if matched:
        return round(total_ty, 4)
    match_full = re.search(r"([\d\.,]+)\s*(?:vnđ|vnd|đ|d)?$", text)
    if match_full:
        num_raw = match_full.group(1).strip()
        # Chuỗi phải có ít nhất 1 dấu phân cách hoặc tối thiểu 6 chữ số
        if "." in num_raw or "," in num_raw or len(re.sub(r"\D", "", num_raw)) >= 6:
            try:
                num_val = _clean_number(num_raw)
                # Nếu số tiền > 10,000,000 VNĐ -> xem là số tiền VNĐ đầy đủ
                if num_val >= 10_000_000:
                    return round(num_val / 1_000_000_000.0, 4)
            except ValueError:
                pass

    return None


def parse_area(val):
    """Bóc tách diện tích đất m2 từ Land Area (ví dụ: '36 m² (3,2x12,0)' -> 36.0)."""
    if pd.isna(val):
        return None
    match = re.search(r"([\d,\.]+)\s*m", str(val))
    if match:
        clean_num = match.group(1).replace(",", ".")
        try:
            return float(clean_num)
        except ValueError:
            return None
    return None


def parse_int_regex(val):
    if pd.isna(val):
        return None
    match = re.search(r"\d+", str(val))
    return int(match.group()) if match else None


def transform_kaggle_to_silver(input_csv_path=None, output_parquet_path=None):
    current_dir = Path(__file__).resolve().parent
    project_root = current_dir.parent.parent  # hcm-house-price-prediction/

    if input_csv_path is None:
        # Đường dẫn mặc định trong kiến trúc thư mục
        input_csv_path = project_root / "data" / "bronze" / "real_estate_listings.csv"
    else:
        input_csv_path = Path(input_csv_path)

    if output_parquet_path is None:
        output_parquet_path = project_root / "data" / "silver" / "silver_kaggle.parquet"
    else:
        output_parquet_path = Path(output_parquet_path)

    if not input_csv_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file Kaggle tại: {input_csv_path}")

    print(f"[*] Đang đọc dữ liệu từ: {input_csv_path}")
    df = pd.read_csv(input_csv_path)
    df = df.dropna(subset=['Price', 'Land Area'])
    # Xử lý NA cho Location và Type of House (Điền chuỗi rỗng để không bị lỗi regex)
    df['Location'] = df['Location'].fillna("")
    df['Type of House'] = df['Type of House'].fillna("")
    df['Legal Documents'] = df['Legal Documents'].fillna("Không rõ")
    # Sửa lỗi encoding cho các cột dạng object/string
    # obj_cols = df.select_dtypes(include=["object", "string", "str"]).columns
    # for col in df:
    #     df[col] = df[col].map(repair_vietnamese_mojibake)

    silver_rows = []
    for idx, row in df.iterrows():
        loc_str = str(row.get("Location", ""))
        loc_parts = [p.strip() for p in loc_str.split(",") if p.strip()]

        ward = loc_parts[0] if len(loc_parts) > 0 else ""
        raw_district = loc_parts[-1] if len(loc_parts) > 1 else ""
        if "-" in raw_district:
            raw_district = raw_district.split("-")[0].strip()
        district = DISTRICT_MAPPING.get(raw_district, raw_district)

        price_bil = parse_price(row.get("Price"))
        area = parse_area(row.get("Land Area"))
        rooms = parse_int_regex(row.get("Bedrooms"))
        toilets = parse_int_regex(row.get("Toilets"))

        floors = row.get("Total Floors")
        try:
            floors = int(floors) if pd.notna(floors) else None
        except Exception:
            floors = None

        house_type = str(row.get("Type of House", "")).lower()
        legal = str(row.get("Legal Documents", "")).lower()

        is_mat_tien = 1 if "mặt tiền" in house_type else 0
        has_so_hong = 1 if "sổ hồng" in legal or "sổ đỏ" in legal else 0

        price_per_m2 = round((price_bil * 1000.0) / area, 2) if (price_bil and area and area > 0) else None

        silver_rows.append({
            "ad_id": f"kaggle_{idx}",
            "source": "kaggle",
            "district": district,
            "ward": ward,
            "price_billion": price_bil,
            "area_m2": area,
            "price_per_m2_million": price_per_m2,
            "rooms": rooms,
            "toilets": toilets,
            "floors": floors,
            "is_mat_tien": is_mat_tien,
            "has_so_hong": has_so_hong
        })

    df_silver = pl.DataFrame(silver_rows)

    df_silver = df_silver.with_columns(
        (pl.col("area_m2") // 20 * 20).alias("area_bin")
    )
    # Điền NA bằng trung vị theo khu vực (Quận) và phân khúc diện tích (Area Bin)
    df_silver = df_silver.with_columns([
        pl.col("rooms").fill_null(pl.median("rooms").over(["district", "area_bin"])),
        pl.col("toilets").fill_null(pl.median("toilets").over(["district", "area_bin"])),
        pl.col("floors").fill_null(pl.median("floors").over(["district", "area_bin"]))
    ])
    # Nếu vẫn còn NA (do cả 1 quận/nhóm diện tích đó không ai nhập), điền mặc định
    df_silver = df_silver.with_columns([
        pl.col("rooms").fill_null(2).cast(pl.Int64),
        pl.col("toilets").fill_null(2).cast(pl.Int64),
        pl.col("floors").fill_null(1).cast(pl.Int64)
    ]).drop("area_bin")
    # Lọc bỏ dòng lỗi diện tích / giá thiếu hoặc không hợp lệ
    total_before = len(df_silver)
    df_silver = df_silver.filter(
        pl.col("price_billion").is_not_null() &
        pl.col("area_m2").is_not_null() &
        (pl.col("area_m2") >= 8.0) & (pl.col("area_m2") <= 2000.0) &
        (pl.col("price_billion") >= 0.3) & (pl.col("price_billion") <= 250.0)
    )

    output_parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df_silver.write_parquet(output_parquet_path, compression="snappy")

    print(f"[OK] Đã lọc {total_before - len(df_silver)} bản ghi lỗi/outlier.")
    print(f"[OK] Đã lưu {len(df_silver)} bản ghi chuẩn hóa vào: {output_parquet_path}")
    return df_silver


if __name__ == "__main__":
    transform_kaggle_to_silver()
