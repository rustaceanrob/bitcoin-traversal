#!/usr/bin/env python3
"""
regress_spend_delta.py

Train a linear regression that predicts spend-height delta from
amount and created height using data in an SQLite `utxo` table.
"""

import argparse
import sqlite3
from pathlib import Path

import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split


def load_dataframe(db_path: Path) -> pd.DataFrame:
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(
            """
            SELECT
                amount,
                created_height,
                spend_height
            FROM utxo
            WHERE spend_height IS NOT NULL
            """,
            conn,
        )
    if df.empty:
        raise ValueError("No spendable rows found in utxo table.")
    df["spend_delta"] = df["spend_height"] - df["created_height"]
    return df


def train_model(df: pd.DataFrame) -> None:
    features = df[["amount", "created_height"]]
    target = df["spend_delta"]

    X_train, X_test, y_train, y_test = train_test_split(
        features, target, test_size=0.2, random_state=42
    )

    model = LinearRegression()
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    print("Linear regression coefficients:")
    print(f"  amount          : {model.coef_[0]:.6f}")
    print(f"  created_height  : {model.coef_[1]:.6f}")
    print(f"Intercept         : {model.intercept_:.6f}")
    print()
    print("Evaluation on hold-out set:")
    print(f"  R^2             : {r2_score(y_test, preds):.4f}")
    print(f"  MAE (blocks)    : {mean_absolute_error(y_test, preds):.2f}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Predict spend-height delta from amount and created height."
    )
    parser.add_argument("database", type=Path, help="Path to the SQLite database file.")
    args = parser.parse_args()

    df = load_dataframe(args.database)
    train_model(df)


if __name__ == "__main__":
    main()
