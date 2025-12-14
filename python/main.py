import argparse
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import SGDRegressor
from sklearn.preprocessing import StandardScaler


SQL = """
SELECT
    amount,
    created_height,
    spend_height
FROM utxo
WHERE spend_height IS NOT NULL
"""


def train_chunked_model(
    db_path: Path,
    chunksize: int = 5_000_000,
    test_size: float = 0.2,
    random_state: int = 42,
) -> None:
    scaler = StandardScaler()
    model = SGDRegressor(
        loss="squared_error",
        penalty="l2",
        alpha=1e-4,
        learning_rate="invscaling",
        eta0=0.01,
        random_state=random_state,
        max_iter=1,
        warm_start=True,
    )

    rng = np.random.default_rng(random_state)
    model_initialized = False
    scaler_initialized = False

    test_count = 0
    test_mean = 0.0
    test_M2 = 0.0
    test_sse = 0.0
    test_mae_sum = 0.0

    with sqlite3.connect(db_path) as conn:
        for chunk_idx, chunk in enumerate(
            pd.read_sql_query(SQL, conn, chunksize=chunksize)
        ):
            amount = chunk["amount"].to_numpy(dtype=np.float64, copy=False)
            created = chunk["created_height"].to_numpy(dtype=np.float64, copy=False)
            spend = chunk["spend_height"].to_numpy(dtype=np.float64, copy=False)

            spend_delta = spend - created
            amount_log = np.log1p(amount)

            X_chunk = np.column_stack((amount_log, created))
            y_chunk = spend_delta

            scaler.partial_fit(X_chunk)
            scaler_initialized = True
            X_scaled = scaler.transform(X_chunk)

            mask = rng.random(len(X_chunk)) < test_size
            X_train = X_scaled[~mask]
            y_train = y_chunk[~mask]

            if X_train.size:
                model.partial_fit(X_train, y_train)
                model_initialized = True

            if model_initialized and mask.any():
                X_test = X_scaled[mask]
                y_test = y_chunk[mask]
                y_pred = model.predict(X_test)

                residuals = y_pred - y_test
                test_sse += np.dot(residuals, residuals)
                test_mae_sum += np.abs(residuals).sum()

                for yi in y_test:
                    test_count += 1
                    delta = yi - test_mean
                    test_mean += delta / test_count
                    test_M2 += delta * (yi - test_mean)

            print(f"Processed chunk {chunk_idx + 1}")

    if not model_initialized or not scaler_initialized:
        print("Model or scaler was never fitted; no data was processed.")
        return

    print("\nLearned model parameters in scaled feature space:")
    print(f"  coef_amount_log     : {model.coef_[0]:.6f}")
    print(f"  coef_created_height : {model.coef_[1]:.6f}")
    print(f"  intercept           : {model.intercept_[0]:.6f}")

    if test_count == 0 or test_M2 == 0:
        print("\nNot enough hold-out data to compute evaluation metrics.")
        return

    mae = test_mae_sum / test_count
    r2 = 1 - (test_sse / test_M2)

    print("\nFinal evaluation on streamed hold-out set:")
    print(f"  MAE (blocks): {mae:.2f}")
    print(f"  R^2        : {r2:.4f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Chunked SGD training on large UTXO table."
    )
    parser.add_argument("db_path", type=Path, help="Path to the SQLite database.")
    parser.add_argument(
        "--chunksize", type=int, default=5_000_000, help="Rows per chunk to stream."
    )
    parser.add_argument(
        "--test_size", type=float, default=0.2, help="Fraction of each chunk for testing."
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="Random seed for stochastic splitting."
    )
    args = parser.parse_args()

    train_chunked_model(
        args.db_path,
        chunksize=args.chunksize,
        test_size=args.test_size,
        random_state=args.seed,
    )
