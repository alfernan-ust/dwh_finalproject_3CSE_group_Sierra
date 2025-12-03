#!/usr/bin/env python3
"""
load_fact_line_items.py

- Uses line_item_data_products.parquet + line_item_data_prices.parquet
- Aligns product rows and quantity rows positionally per order_id
- Loads a clean order-line fact table:

    fact_line_items
        order_id   -> FK to fact_orders(order_id)
        product_id -> FK to dim_product(product_id)
        quantity

NO user_id, merchant_id, staff_id, NO line_item_id.
"""

import os
import sys
import logging

import pandas as pd
import psycopg2
import psycopg2.extras

# ---------- CONFIG ----------
LINE_PRODUCTS = "/dataset/line_item_data_products.parquet"  # order_id, product_id, product_name
LINE_PRICES   = "/dataset/line_item_data_prices.parquet"    # order_id, price, quantity

PG = dict(
    host="postgres",
    database="kestra",
    user="kestra",
    password="k3str4",
)

BATCH = 500

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)


# ---------- HELPERS ----------
def require_file(path: str):
    if not os.path.exists(path):
        raise FileNotFoundError(path)


def clean_str_series(s: pd.Series) -> pd.Series:
    s = s.astype(object).where(s.notnull(), None)

    def norm(v):
        if v is None:
            return None
        if isinstance(v, str):
            v2 = v.strip()
            return v2 if v2 != "" else None
        return v

    return s.apply(norm)


def align_products_quantities(df_p: pd.DataFrame, df_q: pd.DataFrame) -> pd.DataFrame:
    """
    Align product rows and quantity rows positionally per order_id.

    Returns columns:
        order_id, product_id, quantity

    For each order_id:
      - take min(#product_rows, #price_rows)
      - pair them by position (0..n-1)
    """
    logging.info("Aligning products & quantities positionally per order_id")

    df_p = df_p.copy()
    df_q = df_q.copy()

    # normalize order_id
    if "order_id" not in df_p.columns or "order_id" not in df_q.columns:
        raise Exception("Both line item files must contain order_id")

    df_p["order_id"] = clean_str_series(df_p["order_id"])
    df_q["order_id"] = clean_str_series(df_q["order_id"])

    p_groups = df_p.groupby("order_id", sort=False).indices
    q_groups = df_q.groupby("order_id", sort=False).indices

    combined_rows = []
    dropped_unmatched = 0
    matched_pairs = 0

    order_ids = set(p_groups.keys()).union(set(q_groups.keys()))
    for oid in order_ids:
        p_idx = p_groups.get(oid, [])
        q_idx = q_groups.get(oid, [])

        n_p = len(p_idx)
        n_q = len(q_idx)
        n = min(n_p, n_q)

        if n == 0:
            dropped_unmatched += max(n_p, n_q)
            continue

        p_slice = df_p.iloc[p_idx[:n]].reset_index(drop=True)
        q_slice = df_q.iloc[q_idx[:n]].reset_index(drop=True)

        for i in range(n):
            prod_row = p_slice.iloc[i]
            qty_row = q_slice.iloc[i]

            combined_rows.append(
                {
                    "order_id": oid,
                    "product_id": prod_row.get("product_id"),
                    "quantity": qty_row.get("quantity"),
                }
            )

        matched_pairs += n
        if n_p != n_q:
            dropped_unmatched += abs(n_p - n_q)

    logging.info("Aligned pairs: %d, dropped unmatched rows: %d", matched_pairs, dropped_unmatched)

    df = pd.DataFrame(combined_rows)
    df = df.where(pd.notnull(df), None)
    return df


# ---------- DB HELPERS ----------
def ensure_fact_line_items_table(conn):
    """
    fact_line_items:
        order_id   FK → fact_orders(order_id)
        product_id FK → dim_product(product_id)
        quantity
    """
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_line_items (
            order_id   varchar REFERENCES fact_orders(order_id),
            product_id varchar REFERENCES dim_product(product_id),
            quantity   int
        );
        """
    )
    conn.commit()
    cur.close()


def insert_fact_line_items(conn, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        logging.info("No fact_line_items rows to insert")
        return 0

    cur = conn.cursor()

    insert_sql = """
        INSERT INTO fact_line_items (
            order_id, product_id, quantity
        ) VALUES (%s, %s, %s);
    """

    rows = []
    for _, r in df.iterrows():
        qty = r.get("quantity")
        try:
            qty = int(qty) if qty is not None else None
        except Exception:
            qty = None

        rows.append(
            (
                r.get("order_id"),
                r.get("product_id"),
                qty,
            )
        )

    psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=BATCH)
    conn.commit()
    cur.close()

    logging.info("Inserted %d rows into fact_line_items", len(rows))
    return len(rows)


# ---------- MAIN ----------
def main():
    logging.info("Starting load_fact_line_items...")

    # 1. Check files
    require_file(LINE_PRODUCTS)
    require_file(LINE_PRICES)

    # 2. Load parquet files
    logging.info("Reading line item parquet files")
    df_p = pd.read_parquet(LINE_PRODUCTS)  # order_id, product_id, product_name
    df_q = pd.read_parquet(LINE_PRICES)    # order_id, price, quantity

    logging.info("Products rows: %d; Prices rows: %d", len(df_p), len(df_q))

    df_line = align_products_quantities(df_p, df_q)
    logging.info("Combined aligned line rows: %d", len(df_line))

    conn = psycopg2.connect(**PG)
    try:
        ensure_fact_line_items_table(conn)
        inserted = insert_fact_line_items(conn, df_line)
        logging.info("DONE load_fact_line_items. Inserted rows: %d", inserted)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
