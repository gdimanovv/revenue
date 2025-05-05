from models import init_db, upsert_df_revenue_records
from datetime import date, timedelta
import pandas as pd
import os


db = init_db()


# Calculate revenue for the period provided
# start=end if period is a single day 
def calculate_interval_revenue(start, end):
    product_df, sales_df = load_interval_data(start, end)
    date_list = pd.date_range(start=start, end=end).strftime('%Y-%m-%d').tolist()
    for date in date_list:
        date_revenue_df = calculate_date_revenue(product_df=product_df, sales_df=sales_df, execution_date=date)
        upsert_df_revenue_records(db, date_revenue_df)


def load_interval_data(start = None, end = None):
    # load all data
    product_df = pd.read_csv(os.path.join(os.getcwd(), 'data', 'product.csv'), header = 0, dtype={
                     'sku_id': int,
                     'sku_description': str,
                     'price': float
                 },
                 parse_dates=['insert_timestamp_utc'])
    sales_df = pd.read_csv(os.path.join(os.getcwd(), 'data', 'sales.csv'), header = 0, dtype={
                     'sku_id': int,
                     'order_id': str,
                     'sales': int
                 },
                 parse_dates=['orderdate_utc', 'insert_timestamp_utc'])
    
    # keep data only for the selected period 
    if start:
        sales_df = sales_df[(sales_df['orderdate_utc'] >= start)]
    if end:
        sales_df = sales_df[(sales_df['orderdate_utc'] <= end)]
    
    return product_df, sales_df


# Calculate revenue for one day
def calculate_date_revenue(product_df, sales_df, execution_date):
    # Filter sales for the target date
    daily_sales = sales_df[sales_df['orderdate_utc'] == execution_date]

    # Group by sku_id and sum sales
    daily_sales_agg = (
        daily_sales
        .groupby('sku_id', as_index=False)['sales']
        .sum()
    )

    # Join with product prices
    daily_revenue = pd.merge(
        product_df[['sku_id', 'price']],
        daily_sales_agg,
        on='sku_id',
        how='left'
    )

    # Fill missing sales with 0 and calculate revenue
    daily_revenue['sales'] = daily_revenue['sales'].fillna(0).astype(int)
    daily_revenue['revenue'] = daily_revenue['price'] * daily_revenue['sales']
    daily_revenue['date_id'] = execution_date
    daily_revenue['date_id'] = pd.to_datetime(daily_revenue['date_id']).dt.date

    return daily_revenue

if __name__ == "__main__":
    # Generate revenue data for January 2025
    calculate_interval_revenue(start = "2025-01-01", end = "2025-01-31")