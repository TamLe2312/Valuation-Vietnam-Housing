import joblib
import shap
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
from pathlib import Path


def generate_shap_plots():
    # 1. Cấu hình đường dẫn
    project_root = Path(__file__).resolve().parent.parent.parent
    model_path = project_root / "models" / "ridge_optimized.pkl"
    gold_path = project_root / "data" / "gold"
    output_dir = project_root / "models"

    print("[*] Đang tải mô hình và dữ liệu...")
    model = joblib.load(model_path)
    df = pl.read_parquet(gold_path).to_pandas()

    # 2. Tái tạo lại tập X
    df = df[df["price_billion"] <= 50.0]
    if "total_usable_area" not in df.columns:
        df["total_usable_area"] = (df["area_m2"] * df["floors"]).astype(float)
    drop_cols = ["log_price_billion", "log_price_per_m2", "price_billion", "price_per_m2_million"]
    existing_drop_cols = [c for c in drop_cols if c in df.columns]
    X = df.drop(columns=existing_drop_cols)

    # 3. Lấy mẫu dữ liệu đại diện
    sample_size = min(1500, len(X))
    X_sample = X.sample(n=sample_size, random_state=42)

    # 4. Tách Preprocessing nếu model là Pipeline
    # Giả định nếu model là Pipeline có bước mã hóa (TargetEncoder/ColumnTransformer)
    if hasattr(model, "named_steps"):
        print("[*] Phát hiện scikit-learn Pipeline. Đang trích xuất Preprocessor và Ridge...")
        step_names = list(model.named_steps.keys())
        # Bước cuối cùng thường là mô hình hồi quy (Ridge)
        regressor_name = step_names[-1]
        ridge_model = model.named_steps[regressor_name]

        # Các bước phía trước là tiền xử lý
        from sklearn.pipeline import Pipeline
        preprocessor = Pipeline([(name, model.named_steps[name]) for name in step_names[:-1]])

        # Biến đổi X_sample sang dạng số
        X_sample_trans = preprocessor.transform(X_sample)

        # Lấy tên cột sau khi biến đổi nếu có
        if hasattr(preprocessor, "get_feature_names_out"):
            feature_names = preprocessor.get_feature_names_out()
        else:
            feature_names = [f"feature_{i}" for i in range(X_sample_trans.shape[1])]

        X_sample_eval = pd.DataFrame(
            X_sample_trans.toarray() if hasattr(X_sample_trans, "toarray") else X_sample_trans,
            columns=feature_names,
            index=X_sample.index
        )
    else:
        # Nếu model là Ridge thuần (đã qua xử lý trước đó)
        ridge_model = model
        X_sample_eval = X_sample

    # 5. Khởi tạo LinearExplainer
    print("[*] Đang khởi tạo LinearExplainer cho Ridge Regression...")
    # Lấy tập nền (background data) khoảng 100-200 mẫu để tính giá trị kỳ vọng
    background_data = X_sample_eval.sample(n=min(200, len(X_sample_eval)), random_state=42)

    explainer = shap.LinearExplainer(ridge_model, background_data)
    shap_values = explainer(X_sample_eval)

    # 6. Biểu đồ 1: SHAP Summary Plot
    print("[*] Đang vẽ Summary Plot...")
    plt.figure(figsize=(12, 8))
    # Dùng shap_values đối tượng Explanation hoặc mảng giá trị
    shap.summary_plot(shap_values.values, X_sample_eval, show=False)
    plt.title("Tầm quan trọng của các đặc trưng (Ridge Regression)", fontsize=14)
    plt.tight_layout()
    plt.savefig(output_dir / "shap_summary_plot.png", dpi=300)
    plt.close()

    # 7. Biểu đồ 2: SHAP Dependence Plot cho Diện tích
    area_col = [c for c in X_sample_eval.columns if "area_m2" in c]
    if area_col:
        target_col = area_col[0]
        print(f"[*] Đang vẽ Dependence Plot cho {target_col}...")
        plt.figure(figsize=(10, 6))
        shap.dependence_plot(target_col, shap_values.values, X_sample_eval, show=False, interaction_index=None)
        plt.title(f"Tác động của {target_col} lên định giá (Ridge)", fontsize=14)
        plt.tight_layout()
        plt.savefig(output_dir / "shap_dependence_area.png", dpi=300)
        plt.close()

    print(f"[OK] Hoàn tất! Các biểu đồ đã được lưu tại: {output_dir.resolve()}")


if __name__ == "__main__":
    generate_shap_plots()