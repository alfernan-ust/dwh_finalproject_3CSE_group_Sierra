import pandas as pd
import os

# Source Files
PRODUCTS_FILE = "/dataset/line_item_data_products.parquet"
PRICES_FILE   = "/dataset/line_item_data_prices.parquet"

def check_line_items():
    print("--- Verifying Line Item Row Counts ---")
    
    # 1. Load Data
    if not all(os.path.exists(f) for f in [PRODUCTS_FILE, PRICES_FILE]):
        print("Error: One or more source files not found.")
        return

    print("Reading Parquet files...")
    df_p = pd.read_parquet(PRODUCTS_FILE)
    df_q = pd.read_parquet(PRICES_FILE)

    print(f"  Rows in Products File: {len(df_p)}")
    print(f"  Rows in Prices/Qty File: {len(df_q)}")

    # 2. Replicate Alignment Logic (Vectorized)
    # This matches the logic in load_fact_line_items.py
    print("\nAligning data (Positional Merge per Order ID)...")
    
    # Ensure join keys are strings
    df_p['order_id'] = df_p['order_id'].astype(str).str.strip()
    df_q['order_id'] = df_q['order_id'].astype(str).str.strip()

    # Generate position index to align 1st product with 1st price, 2nd with 2nd, etc.
    df_p['pos_idx'] = df_p.groupby('order_id').cumcount()
    df_q['pos_idx'] = df_q.groupby('order_id').cumcount()

    # Inner Join on Order + Position
    # This automatically drops unmatched rows (e.g., if an order has 3 products but only 2 prices)
    df_aligned = pd.merge(
        df_p[['order_id', 'pos_idx']], 
        df_q[['order_id', 'pos_idx']], 
        on=['order_id', 'pos_idx'], 
        how='inner'
    )

    print(f"  Final Calculated Rows (Expected Fact Table Count): {len(df_aligned)}")
    
    # Check for dropped rows
    dropped_p = len(df_p) - len(df_aligned)
    dropped_q = len(df_q) - len(df_aligned)
    
    if dropped_p > 0 or dropped_q > 0:
        print(f"\n⚠️  Note: Alignment dropped unmatched rows.")
        print(f"    - {dropped_p} rows dropped from Products file")
        print(f"    - {dropped_q} rows dropped from Prices file")
    else:
        print("\n✅ Perfect alignment: No rows were dropped.")

if __name__ == "__main__":
    check_line_items()