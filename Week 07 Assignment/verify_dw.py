import sqlite3
import pandas as pd

conn = sqlite3.connect('retail_dw.db')
cursor = conn.cursor()

print("=== 1. Primary Key / Uniqueness ===")
cursor.execute("SELECT COUNT(*), COUNT(DISTINCT order_id) FROM fact_sales;")
total_facts, unique_facts = cursor.fetchone()
print(f"Total Fact rows: {total_facts}, Unique order_id: {unique_facts}")
assert total_facts == unique_facts, "Duplicate order_id found in fact_sales!"

print("\n=== 2. Foreign Key Integrity ===")
cursor.execute("SELECT COUNT(*) FROM fact_sales fs LEFT JOIN dim_customer dc ON fs.customer_key = dc.customer_key WHERE dc.customer_key IS NULL;")
orphan_cust = cursor.fetchone()[0]
print(f"Orphan customer_key in fact_sales: {orphan_cust}")
assert orphan_cust == 0, "Orphan customer_key found!"

cursor.execute("SELECT COUNT(*) FROM fact_sales fs LEFT JOIN dim_product dp ON fs.product_key = dp.product_key WHERE dp.product_key IS NULL;")
orphan_prod = cursor.fetchone()[0]
print(f"Orphan product_key in fact_sales: {orphan_prod}")
assert orphan_prod == 0, "Orphan product_key found!"

cursor.execute("SELECT COUNT(*) FROM fact_sales fs LEFT JOIN dim_date dd ON fs.date_key = dd.date_key WHERE dd.date_key IS NULL;")
orphan_date = cursor.fetchone()[0]
print(f"Orphan date_key in fact_sales: {orphan_date}")
assert orphan_date == 0, "Orphan date_key found!"

print("\n=== 3. Non-negative Constraints ===")
cursor.execute("""
    SELECT COUNT(*) FROM fact_sales 
    WHERE quantity <= 0 
       OR unit_price <= 0 
       OR net_amount < 0 
       OR gross_amount < 0 
       OR discount_pct < 0 
       OR discount_pct > 100;
""")
violations = cursor.fetchone()[0]
print(f"Constraint violations in fact_sales: {violations}")
assert violations == 0, "Constraint violations found in fact_sales!"

print("\n=== 4. Quarantine Reasons Check ===")
cursor.execute("SELECT COUNT(*) FROM quarantine_records WHERE reason_code IS NULL OR TRIM(reason_code) = '';")
empty_reasons = cursor.fetchone()[0]
print(f"Empty reason_code in quarantine: {empty_reasons}")
assert empty_reasons == 0, "Found quarantine record without reason_code!"

cursor.execute("SELECT reason_code, COUNT(*) FROM quarantine_records GROUP BY reason_code ORDER BY COUNT(*) DESC;")
print("\nQuarantine Reason Code Breakdown:")
for code, cnt in cursor.fetchall():
    print(f"  - {code}: {cnt} records")

print("\n=== 5. Pipeline Run Log ===")
df_log = pd.read_sql("SELECT * FROM pipeline_run_log;", conn)
print(df_log[['run_id', 'batch_name', 'rows_read', 'rows_valid', 'rows_rejected', 'rows_duplicated', 'rows_loaded', 'net_sales_loaded', 'status']].to_string(index=False))

print("\n=== 6. Sample Star Schema Join Query ===")
df_sample = pd.read_sql("""
    SELECT 
        fs.order_id,
        dd.full_date,
        dc.customer_name,
        dc.province,
        dp.product_name,
        dp.category,
        fs.quantity,
        fs.unit_price,
        fs.discount_pct,
        fs.gross_amount,
        fs.net_amount,
        fs.payment_method,
        fs.sales_channel
    FROM fact_sales fs
    JOIN dim_customer dc ON fs.customer_key = dc.customer_key
    JOIN dim_product dp ON fs.product_key = dp.product_key
    JOIN dim_date dd ON fs.date_key = dd.date_key
    LIMIT 5;
""", conn)
print(df_sample.to_string(index=False))

conn.close()
print("\n>>> ALL ACCEPTANCE TESTS PASSED SUCCESSFULLY! <<<")
