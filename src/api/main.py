from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from contextlib import asynccontextmanager
import pandas as pd
import numpy as np
import joblib
from pathlib import Path

# Biến toàn cục chứa mô hình
ml_models = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Nạp mô hình vào RAM khi khởi động Server
    project_root = Path(__file__).resolve().parent.parent.parent
    model_path = project_root / "models" / "ridge_optimized.pkl"

    if not model_path.exists():
        raise RuntimeError(f"Không tìm thấy mô hình tại {model_path}")

    ml_models["pricing_model"] = joblib.load(model_path)
    print(f"[OK] Đã nạp mô hình vào RAM từ: {model_path.name}")
    yield
    # Giải phóng tài nguyên khi tắt Server
    ml_models.clear()


app = FastAPI(
    title="Vietnam Housing Pricing API",
    description="API dự đoán giá bất động sản TP.HCM sử dụng mô hình tối ưu Ridge",
    version="1.0.0",
    lifespan=lifespan
)


# 1. Định nghĩa Schema dữ liệu đầu vào (Data Validation)
class PropertyFeatures(BaseModel):
    district: str = Field(..., description="Tên Quận/Huyện")
    ward: str = Field(default="", description="Tên Phường/Xã")
    area_m2: float = Field(..., gt=0, description="Diện tích đất (m2)")
    rooms: int = Field(..., gt=0, description="Số phòng ngủ")
    toilets: int = Field(..., gt=0, description="Số nhà vệ sinh")
    floors: int = Field(..., gt=0, description="Số tầng")
    is_mat_tien: bool = Field(default=False, description="Nhà mặt tiền (True) hay hẻm (False)")
    has_so_hong: bool = Field(default=True, description="Có sổ hồng")


# 2. Định nghĩa Schema dữ liệu trả về
class PredictionResponse(BaseModel):
    predicted_price_billion: float
    price_per_m2_million: float
    status: str


# 3. Endpoint nhận yêu cầu dự đoán
@app.post("/predict", response_model=PredictionResponse)
async def predict_price(property_data: PropertyFeatures):
    try:
        model = ml_models["pricing_model"]
        # Tạo biến phái sinh (Feature Engineering)
        total_usable_area = float(property_data.area_m2 * property_data.floors)
        area_per_room = property_data.area_m2 / property_data.rooms
        toilets_per_room = property_data.toilets / property_data.rooms

        input_dict = {
            "district": property_data.district,
            "ward": property_data.ward,
            "area_m2": property_data.area_m2,
            "total_usable_area": total_usable_area,
            "rooms": property_data.rooms,
            "toilets": property_data.toilets,
            "floors": property_data.floors,
            "is_mat_tien": 1 if property_data.is_mat_tien else 0,
            "has_so_hong": 1 if property_data.has_so_hong else 0,
            "area_per_room": area_per_room,
            "toilets_per_room": toilets_per_room
        }

        df_input = pd.DataFrame([input_dict])

        raw_pred = float(model.predict(df_input)[0])
        pred_price = float(np.expm1(raw_pred))
        price_per_m2 = (pred_price * 1000) / property_data.area_m2

        return PredictionResponse(
            predicted_price_billion=round(pred_price, 3),
            price_per_m2_million=round(price_per_m2, 2),
            status="success"
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    return {"status": "ok", "model_loaded": "pricing_model" in ml_models}


@app.get("/model-info")
async def get_model_info():
    model = ml_models["pricing_model"]

    features = []
    # 1. Thử lấy nếu là mô hình trực tiếp (Ridge trơn)
    if hasattr(model, "feature_names_in_"):
        features = list(model.feature_names_in_)

    # 2. Thử lấy nếu mô hình là Pipeline
    elif hasattr(model, "named_steps"):
        first_step_name = list(model.named_steps.keys())[0]
        first_step_obj = model.named_steps[first_step_name]
        if hasattr(first_step_obj, "feature_names_in_"):
            features = list(first_step_obj.feature_names_in_)

    return {
        "model_type": str(type(model)),
        "expected_features": features,
        "feature_count": len(features)
    }