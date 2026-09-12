import streamlit as st
import pandas as pd
import numpy as np
import joblib
from pathlib import Path

st.set_page_config(page_title="Định Giá BĐS TP.HCM", page_icon="🏠", layout="centered")

@st.cache_resource
def load_model():
    project_root = Path(__file__).resolve().parent.parent.parent
    model_path = project_root / "models" / "ridge_optimized.pkl"
    return joblib.load(model_path)


def main():
    st.title("🏠 Hệ Thống Định Giá Bất Động Sản TP.HCM")
    st.markdown("Dự đoán giá nhà dựa trên mô hình Machine Learning **Ridge Regression**.")

    model = load_model()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Vị trí & Quy mô")
        district = st.selectbox(
            "Quận / Huyện",
            options=["Quận 1", "Quận 3", "Quận 4", "Quận 5", "Quận 6", "Quận 7", "Quận 8",
                     "Quận 10", "Quận 11", "Quận 12", "Quận Bình Thạnh", "Quận Gò Vấp",
                     "Quận Phú Nhuận", "Quận Tân Bình", "Quận Tân Phú", "Quận Bình Tân",
                     "Thành phố Thủ Đức", "Huyện Nhà Bè", "Huyện Hóc Môn", "Huyện Bình Chánh", "Huyện Củ Chi"]
        )
        ward = st.text_input("Phường / Xã", value="Phường Bến Nghé")
        area_m2 = st.number_input("Diện tích đất (m2)", min_value=10.0, max_value=2000.0, value=50.0, step=1.0)
    with col2:
        st.subheader("Cấu trúc & Pháp lý")
        rooms = st.number_input("Số phòng ngủ", min_value=1, max_value=20, value=2, step=1)
        toilets = st.number_input("Số nhà vệ sinh", min_value=1, max_value=20, value=2, step=1)
        floors = st.number_input("Số tầng", min_value=1, max_value=20, value=1, step=1)

        is_mat_tien = st.checkbox("Là nhà mặt tiền?")
        has_so_hong = st.checkbox("Đã có Sổ Hồng/Sổ Đỏ?", value=True)

    if st.button("🚀 Dự đoán Giá", use_container_width=True):
        # Tính toán Feature Engineering On-the-fly
        total_usable_area = float(area_m2 * floors)
        area_per_room = float(area_m2 / rooms) if rooms > 0 else 0.0
        toilets_per_room = float(toilets / rooms) if rooms > 0 else 0.0

        # Đóng gói dữ liệu đầu vào chuẩn xác với cấu trúc lúc huấn luyện
        input_data = pd.DataFrame([{
            "district": str(district),
            "ward": str(ward),
            "area_m2": float(area_m2),
            "total_usable_area": total_usable_area,
            "rooms": int(rooms),
            "toilets": int(toilets),
            "floors": int(floors),
            "is_mat_tien": int(1 if is_mat_tien else 0),
            "has_so_hong": int(1 if has_so_hong else 0),
            "area_per_room": area_per_room,
            "toilets_per_room": toilets_per_room
        }])

        with st.spinner("Đang xử lý thuật toán..."):
            pred_price = float(model.predict(input_data)[0])

        st.success("Tính toán hoàn tất!")
        st.markdown(f"<h2 style='text-align: center; color: #2e7bcf;'>Giá trị ước tính: {pred_price:.2f} Tỷ VNĐ</h2>",
                    unsafe_allow_html=True)
        st.markdown(
            f"<p style='text-align: center;'>Đơn giá tham khảo: <b>{(pred_price * 1000 / area_m2):.1f} Triệu/m2</b></p>",
            unsafe_allow_html=True)


if __name__ == "__main__":
    main()