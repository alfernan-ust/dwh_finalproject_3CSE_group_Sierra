import pandas as pd
from pathlib import Path

IN = "/dataset/extracted"
OUT = "/dataset/transformed"

Path(OUT).mkdir(parents=True, exist_ok=True)

df = pd.read_parquet(f"{IN}/product_list.parquet")

# Handle empty DataFrame gracefully
if df.empty:
    print("Warning: Empty product_list data. Creating empty dim_product.parquet")
    pd.DataFrame().to_parquet(f"{OUT}/dim_product.parquet", index=False)
    print("[SUCCESS] transform_business → dim_product.parquet (empty)")
    exit(0)

df.columns = df.columns.str.strip().str.lower()


df["product_id"] = df["product_id"].astype(str).replace({"": None, "nan": None})
df["product_name"] = df["product_name"].astype(str).replace({"": None, "nan": None})
df["product_type"] = df["product_type"].astype(str).replace({"": None, "nan": None})
df["price"] = pd.to_numeric(df["price"], errors="coerce")


# Mark all duplicates (including the one we'll keep) to track that duplicates existed
has_duplicates = df["product_id"].duplicated(keep=False)
df["is_duplicate"] = has_duplicates

required = ["product_id", "product_name", "product_type", "price"]

# Check for null values in ALL fields (excluding quality flags)
data_cols = [c for c in df.columns if c not in ['is_duplicate', 'is_incomplete', 'incomplete_reason', 'is_inferred']]
df["is_incomplete"] = df[data_cols].isnull().any(axis=1)

# Set incomplete_reason based on what's missing
incomplete_reasons = []
for idx, row in df.iterrows():
    if not row["is_incomplete"]:
        incomplete_reasons.append(None)
    else:
        missing = [col for col in data_cols if pd.isnull(row[col])]
        incomplete_reasons.append(f"Missing: {', '.join(missing)}")

df["incomplete_reason"] = incomplete_reasons

# All records from source data are not inferred (only inferred when created by fact load)
df["is_inferred"] = False

cols = [
    "product_id",
    "product_name",
    "product_type",
    "price",
    "is_duplicate",
    "is_inferred",
    "is_incomplete",
    "incomplete_reason",
]

df = df[cols]

df.to_parquet(f"{OUT}/dim_product.parquet", index=False)

print("[SUCCESS] transform_business → dim_product.parquet")
