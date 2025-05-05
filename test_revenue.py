import pytest
import pandas as pd
from revenue import calculate_date_revenue

@pytest.fixture
def sample_product_data():
    return pd.DataFrame({
        'sku_id': [1001, 1002, 1003],
        'sku_description': ['product1', 'product2', 'product3'],
        'price': [10.0, 20.0, 15.0],
        'insert_timestamp_utc': pd.to_datetime(['2025-01-01', '2025-01-01', '2025-01-01'])
    })


@pytest.fixture
def sample_sales_data():
    return pd.DataFrame({
        'sku_id': [1001, 1001, 1002, 1003, 1003],
        'order_id': ['order1', 'order2', 'order3', 'order4', 'order5'],
        'sales': [2, 3, 1, 4, 2],
        'orderdate_utc': pd.to_datetime(['2025-01-01', '2025-01-01', '2025-01-02', '2025-01-03', '2025-01-03']),
        'insert_timestamp_utc': pd.to_datetime(['2025-01-01', '2025-01-01', '2025-01-02', '2025-01-03', '2025-01-03'])
    })


def test_calculate_date_revenue(sample_product_data, sample_sales_data):
    # Tests output dataframe format, revenue and inclusion of products without sales
    result_df = calculate_date_revenue(
        product_df=sample_product_data,
        sales_df=sample_sales_data,
        execution_date='2025-01-01'
    )
    
    sku_1001 = result_df[result_df['sku_id'] == 1001]
    assert sku_1001['sales'].iloc[0] == 5
    assert sku_1001['price'].iloc[0] == 10.0
    assert sku_1001['revenue'].iloc[0] == 50.0
    
    # Test for a date with no sales
    no_sales_df = calculate_date_revenue(
        product_df=sample_product_data,
        sales_df=sample_sales_data,
        execution_date='2025-01-10'
    )
    
    # Should have all products but zero sales
    assert len(no_sales_df) == len(sample_product_data)
    assert (no_sales_df['sales'] == 0).all()
    assert (no_sales_df['revenue'] == 0).all()