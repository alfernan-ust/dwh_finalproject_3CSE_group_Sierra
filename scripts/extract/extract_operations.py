from dataframe import set_frame, append_files
import glob

# <-- LEGACY DATA
            # # Line item data prices
            # line_item_data_prices = glob.glob("/dataset/OperationsDepartment/line_item_data_prices*")
            # line_item_data_prices.sort()
            # df = set_frame(line_item_data_prices[0])
            # line_item_data_prices.pop(0)
            # df = append_files(df, line_item_data_prices)

            # # removes non-numbers in 'quantity' column
            # df.replace(to_replace={'quantity': '[^0-9]'}, value="", inplace=True, regex=True)
            # df.to_parquet("line_item_data_prices.parquet")

            # # Line item data products
            # line_item_data_products = glob.glob("/dataset/OperationsDepartment/line_item_data_products*")
            # line_item_data_products.sort()
            # df = set_frame(line_item_data_products[0])
            # line_item_data_products.pop(0)
            # df = append_files(df, line_item_data_products)

            # df.to_parquet("line_item_data_products.parquet")
# -->            
from dataframe import set_frame, append_files
import glob

# Line item prices
prices = glob.glob("/dataset/OperationsDepartment/line_item_data_prices*")
prices.sort()
df = set_frame(prices[0])
prices.pop(0)
df = append_files(df, prices)

df.to_parquet('/dataset/extracted/line_item_data_prices.parquet')

# Line item products
products = glob.glob("/dataset/OperationsDepartment/line_item_data_products*")
products.sort()
df = set_frame(products[0])
products.pop(0)
df = append_files(df, products)

df.to_parquet('/dataset/extracted/line_item_data_products.parquet')

# Orders
orders = glob.glob("/dataset/OperationsDepartment/order_data*")
orders.sort()
df = set_frame(orders[0])
orders.pop(0)
df = append_files(df, orders)

df.to_parquet('/dataset/extracted/output_order_data.parquet')

# Order delays
delays = glob.glob("/dataset/OperationsDepartment/order_delay*")
delays.sort()
df = set_frame(delays[0])
if len(delays) > 1:
    delays.pop(0)
    df = append_files(df, delays)

df.to_parquet("/dataset/extracted/order_delays.parquet", index=False)
