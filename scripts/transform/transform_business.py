import pandas as pd
from pathlib import Path

IN = "/dataset/extracted"
OUT = "/dataset/transformed"

Path(OUT).mkdir(parents=True, exist_ok=True)

df = pd.read_parquet(f"{IN}/product_list.parquet")

df.columns = df.columns.str.strip().str.lower()


df["product_id"] = df["product_id"].astype(str).replace({"": None, "nan": None})
df["product_name"] = df["product_name"].astype(str).replace({"": None, "nan": None})
df["product_type"] = df["product_type"].astype(str).replace({"": None, "nan": None})
df["price"] = pd.to_numeric(df["price"], errors="coerce")


df["is_duplicate"] = df.duplicated(subset=["product_id"], keep="last")


required = ["product_id", "product_name", "product_type", "price"]

df["is_incomplete"] = df[required].isnull().any(axis=1)
df["incomplete_reason"] = None
df.loc[df["is_incomplete"], "incomplete_reason"] = "Missing Required Attributes"

cols = [
    "product_id",
    "product_name",
    "product_type",
    "price",
    "is_duplicate",
    "is_incomplete",
    "incomplete_reason",
]

df = df[cols]

df.to_parquet(f"{OUT}/dim_product.parquet", index=False)

print("[SUCCESS] transform_business → dim_product.parquet")
