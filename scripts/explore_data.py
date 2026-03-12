"""
Quick exploration of the Brazilian E-Commerce dataset
Just trying to understand what we're working with before diving in
"""

import pandas as pd
import os

def explore_file(filename):
    """Look at a single CSV file and print some useful info"""
    filepath = os.path.join('data', filename)
    
    if not os.path.exists(filepath):
        print(f"Hmm, can't find {filename}... skipping")
        return
    
    # Load the data
    df = pd.read_csv(filepath)
    
    print(f"\n{'='*70}")
    print(f"File: {filename}")
    print(f"{'='*70}")
    
    # Basic stats
    print(f"\nRows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")
    
    # Show columns with their data types and check for nulls
    print("\nColumns:")
    for col in df.columns:
        null_count = df[col].isnull().sum()
        null_pct = (null_count / len(df)) * 100 if len(df) > 0 else 0
        
        # Only mention nulls if there are any
        null_info = f" ({null_count:,} nulls, {null_pct:.1f}%)" if null_count > 0 else ""
        print(f"  - {col}: {df[col].dtype}{null_info}")
    
    # Show first few rows
    print("\nFirst 3 rows:")
    print(df.head(3).to_string(index=False))
    
    return df

def main():
    print("\n" + "="*70)
    print("EXPLORING BRAZILIAN E-COMMERCE DATA")
    print("="*70)
    
    # Files we care about
    files_to_check = [
        'olist_orders_dataset.csv',
        'olist_order_items_dataset.csv', 
        'olist_customers_dataset.csv',
        'olist_products_dataset.csv',
        'olist_sellers_dataset.csv',
        'olist_order_payments_dataset.csv',
        'olist_order_reviews_dataset.csv'
    ]
    
    # Quick look at each file
    for file in files_to_check:
        explore_file(file)
    
    # Some quick analysis of the orders
    print("\n" + "="*70)
    print("QUICK ANALYSIS")
    print("="*70)
    
    try:
        orders = pd.read_csv('data/olist_orders_dataset.csv')
        
        # Date range
        orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
        print(f"\nDate range: {orders['order_purchase_timestamp'].min()} to {orders['order_purchase_timestamp'].max()}")
        
        # Order statuses
        print("\nOrder statuses:")
        print(orders['order_status'].value_counts())
        
        # Quick stats
        order_items = pd.read_csv('data/olist_order_items_dataset.csv')
        total_revenue = order_items['price'].sum()
        print(f"\nTotal revenue: R$ {total_revenue:,.2f}")
        print(f"Average order value: R$ {order_items.groupby('order_id')['price'].sum().mean():,.2f}")
        
    except Exception as e:
        print(f"Couldn't do quick analysis: {e}")
    
    print("\n" + "="*70)
    print("Done! Data looks good.")
    print("="*70 + "\n")

if __name__ == "__main__":
    main()