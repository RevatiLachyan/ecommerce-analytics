import snowflake.connector

# Based on your URL: https://app.snowflake.com/yxfldfk/nx25330/
# Try these formats

account_formats = [
    'nx25330',              # Just account name
    'yxfldfk.nx25330',      # Org.Account with dot
    'yxfldfk-nx25330',      # Org-Account with hyphen
]

USER = 'REVATLMADES'           # ← Replace
PASSWORD = 'Northeastern3!!'       # ← Replace

print("="*70)
print("TESTING ACCOUNT FORMATS FOR: yxfldfk/nx25330")
print("="*70)

for account in account_formats:
    print(f"\n▶ Trying: {account}")
    try:
        conn = snowflake.connector.connect(
            user=USER,
            password=PASSWORD,
            account=account,
            warehouse='COMPUTE_WH',
            database='ECOMMERCE_ANALYTICS'
        )
        
        print(f"✅ ✅ ✅ SUCCESS! ✅ ✅ ✅")
        print(f"\nWorking account format: {account}\n")
        
        # Verify connection
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_WAREHOUSE()")
        result = cur.fetchone()
        
        print(f"Logged in as: {result[0]}")
        print(f"Database: {result[1]}")
        print(f"Warehouse: {result[2]}")
        
        conn.close()
        
        print("\n" + "="*70)
        print("✅ USE THIS IN YOUR PROFILES.YML:")
        print("="*70)
        print(f"account: {account}")
        print("="*70)
        break
        
    except Exception as e:
        error_msg = str(e)
        if "404" in error_msg:
            print(f"   ❌ 404 Error - Wrong account format")
        elif "Incorrect username or password" in error_msg:
            print(f"   ⚠️  Account format is CORRECT but username/password wrong")
            print(f"   ✅ USE THIS FORMAT: {account}")
            break
        else:
            print(f"   ❌ Error: {error_msg[:100]}")

print("\nDone testing.")