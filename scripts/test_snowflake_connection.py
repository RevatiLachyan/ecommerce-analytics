"""
Test Snowflake connection - make sure credentials work
"""

import snowflake.connector

# REPLACE THESE WITH YOUR ACTUAL VALUES
ACCOUNT = 'yxfldfk-nx25330'  # e.g., 'abc12345.us-east-1'
USER = 'revatilmades'
PASSWORD = 'Northeastern3!'
WAREHOUSE = 'COMPUTE_WH'
DATABASE = 'ECOMMERCE_ANALYTICS'

print("Testing Snowflake connection...")
print(f"Account: {ACCOUNT}")
print(f"User: {USER}")
print(f"Database: {DATABASE}")
print()

try:
    # Try to connect
    conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        role='ACCOUNTADMIN',
        warehouse='COMPUTE_WH',
        database='ECOMMERCE_ANALYTICS',
        schema='RAW'
    )
    
    
    # Run a simple query
    cur = conn.cursor()
    cur.execute("SELECT CURRENT_VERSION()")
    version = cur.fetchone()
    
    print("✅ CONNECTION SUCCESSFUL!")
    print(f"Snowflake version: {version[0]}")
    
    # Check our tables
    cur.execute("SHOW TABLES IN SCHEMA RAW")
    tables = cur.fetchall()
    
    print(f"\nTables found in RAW schema: {len(tables)}")
    for table in tables:
        print(f"  - {table[1]}")
    
    conn.close()
    print("\n✅ All good! Your credentials work.")
    
except Exception as e:
    print("❌ CONNECTION FAILED!")
    print(f"Error: {str(e)}")
    print("\nCommon issues:")
    print("1. Account format wrong - try with/without region")
    print("2. Password incorrect - check for special characters")
    print("3. User doesn't have access to COMPUTE_WH warehouse")