import pandas as pd
import os

#Staging Layer- Cleans data and converts to new CSV files
def prepare_customers():
    """Customers table - pretty clean already"""
    print("\nPreparing customers...")
    df = pd.read_csv('data/olist_customers_dataset.csv')
    
    # Just save it as-is, it's already clean
    df.to_csv('data/prepared/customers.csv', index=False)
    print(f"  ✓ {len(df):,} customers ready")
    return len(df)

def prepare_orders():
    """Orders table - need to handle dates"""
    print("\nPreparing orders...")
    df = pd.read_csv('data/olist_orders_dataset.csv')
    
    # Convert all timestamp columns to proper datetime
    date_columns = [
        'order_purchase_timestamp',
        'order_approved_at',
        'order_delivered_carrier_date', 
        'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]
    
    for col in date_columns:
        #becomes NaT instead of crashing
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    df.to_csv('data/prepared/orders.csv', index=False, date_format='%Y-%m-%d %H:%M:%S')
    print(f"  ✓ {len(df):,} orders ready")
    return len(df)

def prepare_order_items():
    """Order items - also need to handle dates"""
    print("\nPreparing order items...")
    df = pd.read_csv('data/olist_order_items_dataset.csv')
    
    # Fix the shipping limit date
    df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'], errors='coerce')
    
    df.to_csv('data/prepared/order_items.csv', index=False, date_format='%Y-%m-%d %H:%M:%S')
    print(f"  ✓ {len(df):,} order items ready")
    return len(df)

def prepare_products():
    """Products table"""
    print("\nPreparing products...")
    df = pd.read_csv('data/olist_products_dataset.csv')
    
    # Some products don't have categories - that's ok, we'll handle in dbt
    df.to_csv('data/prepared/products.csv', index=False)
    print(f"  ✓ {len(df):,} products ready")
    
    # Quick note about missing categories
    missing = df['product_category_name'].isnull().sum()
    if missing > 0:
        print(f"  ⚠ Note: {missing} products missing category name")
    
    return len(df)

def prepare_sellers():
    """Sellers table"""
    print("\nPreparing sellers...")
    df = pd.read_csv('data/olist_sellers_dataset.csv')
    
    df.to_csv('data/prepared/sellers.csv', index=False)
    print(f"  ✓ {len(df):,} sellers ready")
    return len(df)

def prepare_payments():
    """Payments table"""
    print("\nPreparing payments...")
    df = pd.read_csv('data/olist_order_payments_dataset.csv')
    
    df.to_csv('data/prepared/payments.csv', index=False)
    print(f"  ✓ {len(df):,} payment records ready")
    return len(df)

def prepare_reviews():
    """Reviews table - handle dates and text"""
    print("\nPreparing reviews...")
    df = pd.read_csv('data/olist_order_reviews_dataset.csv')
    
    # Fix date columns
    df['review_creation_date'] = pd.to_datetime(df['review_creation_date'], errors='coerce')
    df['review_answer_timestamp'] = pd.to_datetime(df['review_answer_timestamp'], errors='coerce')
    
    # Some reviews have really long text - truncate to avoid issues
    if 'review_comment_message' in df.columns:
        df['review_comment_message'] = df['review_comment_message'].astype(str).str[:5000]
    
    df.to_csv('data/prepared/reviews.csv', index=False, date_format='%Y-%m-%d %H:%M:%S')
    print(f"  ✓ {len(df):,} reviews ready")
    return len(df)

def main():
    print("\n" + "="*70)
    print("PREPARING DATA FOR SNOWFLAKE")
    print("="*70)
    
    # Create output directory
    os.makedirs('data/prepared', exist_ok=True)
    
    # Process each table
    counts = {}
    counts['customers'] = prepare_customers()
    counts['orders'] = prepare_orders()
    counts['order_items'] = prepare_order_items()
    counts['products'] = prepare_products()
    counts['sellers'] = prepare_sellers()
    counts['payments'] = prepare_payments()
    counts['reviews'] = prepare_reviews()
    
    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    total_rows = sum(counts.values())
    print(f"\nTotal rows prepared: {total_rows:,}")
    print("\nBreakdown:")
    for table, count in counts.items():
        print(f"  - {table}: {count:,}")
    
    print(f"\n✓ All files saved to: data/prepared/")
    print("\nReady to load into Snowflake!")
    print("="*70 + "\n")

if __name__ == "__main__":
    main()