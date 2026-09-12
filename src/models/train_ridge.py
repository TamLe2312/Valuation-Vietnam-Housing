from pathlib import Path
import warnings
import joblib
import numpy as np
import optuna
import pandas as pd
import polars as pl
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, mean_squared_error
from sklearn.model_selection import KFold, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, TargetEncoder

warnings.filterwarnings('ignore')

storage_options = {
    "aws_access_key_id": "admin",
    "aws_secret_access_key": "password123",
    "endpoint_url": "http://127.0.0.1:9000",
    "allow_http": "true"
}

s3_gold_path = "s3://datalake/gold/master_features/"


def safe_expm1(x):
    """
    Hàm này giờ đây chỉ dùng cục bộ trong file train để tính toán đánh giá (Metrics).
    Nó SẼ KHÔNG bị lưu vào file .pkl nữa.
    """
    return np.expm1(np.clip(x, a_min=None, a_max=10.0))


def load_and_prepare_data():
    df = pl.read_parquet(s3_gold_path, storage_options=storage_options).to_pandas()

    # Feature Engineering phái sinh
    df["total_usable_area"] = df["area_m2"] * df["floors"]
    if "rooms" in df.columns:
        df["area_per_room"] = df["area_m2"] / df["rooms"].replace(0, 1)
        df["toilets_per_room"] = df["toilets"] / df["rooms"].replace(0, 1)

    y = df["price_billion"]

    # Loại bỏ nhãn gốc và các cột phái sinh liên quan đến giá khỏi X
    drop_cols = ["price_billion", "log_price_billion", "price_per_m2_million", "log_price_per_m2"]
    existing_drop_cols = [c for c in drop_cols if c in df.columns]
    X = df.drop(columns=existing_drop_cols)

    return train_test_split(X, y, test_size=0.2, random_state=42)


def build_pipeline(X_sample, alpha, solver, smooth):
    numeric_features = X_sample.select_dtypes(include=['int64', 'float64']).columns.tolist()
    categorical_features = X_sample.select_dtypes(exclude=['int64', 'float64']).columns.tolist()

    numeric_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler())
    ])

    categorical_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='most_frequent')),
        ('target_encoder', TargetEncoder(target_type='continuous', smooth=smooth))
    ])

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, numeric_features),
            ('cat', categorical_transformer, categorical_features)
        ])

    # Chỉ dùng Ridge thuần, KHÔNG BỌC TransformedTargetRegressor nữa
    base_ridge = Ridge(alpha=alpha, solver=solver, random_state=42)

    return Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('regressor', base_ridge)
    ])


def objective(trial, X_train, y_train, n_splits=5):
    alpha = trial.suggest_float("alpha", 1e-3, 1e4, log=True)
    solver = trial.suggest_categorical("solver", ["auto", "lsqr", "sparse_cg", "sag"])
    smooth = trial.suggest_float("smooth", 1.0, 100.0, log=True)

    kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
    fold_mapes = []

    for fold, (train_idx, val_idx) in enumerate(kf.split(X_train, y_train)):
        X_tr, X_val = X_train.iloc[train_idx], X_train.iloc[val_idx]
        y_tr, y_val = y_train.iloc[train_idx], y_train.iloc[val_idx]

        # TỰ ĐỘNG CHUYỂN LOG ở bên ngoài pipeline
        y_tr_log = np.log1p(y_tr)

        pipeline = build_pipeline(X_tr, alpha, solver, smooth)
        pipeline.fit(X_tr, y_tr_log)  # Fit bằng giá Log

        # Pipeline dự đoán ra giá Log
        y_val_pred_log = pipeline.predict(X_val)

        # Tự hoàn nguyên để tính MAPE
        y_val_pred = safe_expm1(y_val_pred_log)

        score = mean_absolute_percentage_error(y_val, y_val_pred)
        fold_mapes.append(score)

    return np.mean(fold_mapes)


def run_optimization():
    X_train, X_test, y_train, y_test = load_and_prepare_data()

    print(f"[*] Kích thước tập Train: {X_train.shape[0]} | Tập Test: {X_test.shape[0]}")
    print("[*] Bắt đầu dò tìm siêu tham số Ridge với 5-Fold Cross-Validation qua 50 vòng Optuna...")

    study = optuna.create_study(direction="minimize")
    study.optimize(lambda trial: objective(trial, X_train, y_train, n_splits=5), n_trials=50)

    print("\n" + "=" * 50)
    print("THÔNG SỐ RIDGE TỐT NHẤT (QUA 5-FOLD CV):")
    best_params = study.best_params
    for key, value in best_params.items():
        print(f"  - {key}: {value}")

    print(f"\nMAPE TRUNG BÌNH CV TỐI ƯU: {study.best_value * 100:.2f}%")
    print("=" * 50)

    print("\n[*] Đang huấn luyện lại Final Pipeline trên toàn bộ tập Train...")
    final_pipeline = build_pipeline(X_train, alpha=best_params["alpha"], solver=best_params["solver"],
                                    smooth=best_params["smooth"])

    # Fit Final model với dữ liệu đã log
    y_train_log = np.log1p(y_train)
    final_pipeline.fit(X_train, y_train_log)

    # Dự đoán và hoàn nguyên
    y_test_pred_log = final_pipeline.predict(X_test)
    y_test_pred = safe_expm1(y_test_pred_log)

    test_mape = mean_absolute_percentage_error(y_test, y_test_pred) * 100
    test_mae = mean_absolute_error(y_test, y_test_pred)
    test_rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))

    print(f"\nKẾT QUẢ TRÊN HOLDOUT TEST SET:")
    print(f" - MAPE Test: {test_mape:.2f}%")
    print(f" - MAE Test:  {test_mae:.3f} Tỷ")
    print(f" - RMSE Test: {test_rmse:.3f} Tỷ")

    project_root = Path(__file__).resolve().parent.parent.parent
    models_dir = project_root / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    model_path = models_dir / "ridge_optimized.pkl"

    joblib.dump(final_pipeline, model_path)
    print(f"\n[OK] Đã lưu mô hình tối ưu đạt chuẩn CV tại: {model_path}")


if __name__ == "__main__":
    run_optimization()