"""Challenge tasks A and B for Week 09 Lab.
Run: python challenge.py
"""
import shutil
import sqlite3
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent

def run_challenge_a():
    print("=" * 60)
    print("Challenge A: Analysis on extended.db")
    print("=" * 60)
    db_path = ROOT / 'data' / 'extended.db'
    if not db_path.exists():
        print(f"Error: {db_path} not found.")
        return

    with sqlite3.connect(db_path.as_uri() + '?mode=ro', uri=True) as con:
        # 1. Top 3 Stores by Revenue
        q_top3 = """
        SELECT
            store_name,
            province,
            region,
            SUM(amount) AS total_revenue
        FROM sales
        GROUP BY store_name, province, region
        ORDER BY total_revenue DESC
        LIMIT 3;
        """
        df_top3 = pd.read_sql_query(q_top3, con)
        print("\n--- 1. Top 3 Stores by Total Revenue ---")
        print(df_top3.to_string(index=False))

        # 2. Monthly AOV
        q_monthly_aov = """
        SELECT
            month,
            SUM(amount) AS revenue,
            COUNT(DISTINCT order_id) AS orders,
            ROUND(1.0 * SUM(amount) / COUNT(DISTINCT order_id), 2) AS monthly_aov
        FROM sales
        GROUP BY month
        ORDER BY month;
        """
        df_monthly = pd.read_sql_query(q_monthly_aov, con)
        print("\n--- 2. Monthly AOV ---")
        print(df_monthly.to_string(index=False))

        # 3. Overall AOV vs Average of Monthly AOVs
        q_overall = """
        SELECT
            SUM(amount) AS total_revenue,
            COUNT(DISTINCT order_id) AS total_orders,
            ROUND(1.0 * SUM(amount) / COUNT(DISTINCT order_id), 2) AS overall_aov
        FROM sales;
        """
        df_overall = pd.read_sql_query(q_overall, con)
        overall_aov = df_overall.iloc[0]['overall_aov']
        simple_avg_monthly_aov = round(df_monthly['monthly_aov'].mean(), 2)

        print("\n--- 3. Comparison of AOV ---")
        print(f"Total Revenue across period: {df_overall.iloc[0]['total_revenue']:,} บาท")
        print(f"Total Distinct Orders:       {df_overall.iloc[0]['total_orders']:,} ออเดอร์")
        print(f"Overall AOV (Revenue / Orders):       {overall_aov:.2f} บาท/ออเดอร์")
        print(f"Simple Average of Monthly AOVs:       {simple_avg_monthly_aov:.2f} บาท/ออเดอร์")
        print("Note: ค่าเฉลี่ยตรงๆ (Simple Average) ของ AOV รายเดือนไม่เท่ากับ Overall AOV")
        print("เพราะแต่ละเดือนมีจำนวนออเดอร์ (ตัวหาร/น้ำหนัก) ไม่เท่ากัน (603, 586, 611 ออเดอร์)")
        print("AOV เป็น Non-additive metric การรวมข้ามเดือนจึงต้องคำนวณจากผลรวมยอดขายทั้งหมดหารด้วยผลรวมออเดอร์ทั้งหมด")

def run_challenge_b():
    print("\n" + "=" * 60)
    print("Challenge B: Extending warehouse.db into challenge.db")
    print("=" * 60)
    src_db = ROOT / 'data' / 'warehouse.db'
    dst_db = ROOT / 'data' / 'challenge.db'

    # 1. Copy warehouse.db to challenge.db
    shutil.copyfile(src_db, dst_db)
    print(f"Copied {src_db.name} -> {dst_db.name}")

    with sqlite3.connect(dst_db) as con:
        # Enable Foreign Keys
        con.execute("PRAGMA foreign_keys = ON;")

        # Check before adding new data
        cur = con.cursor()
        rows_before = cur.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
        amt_before = cur.execute("SELECT SUM(quantity * unit_price) FROM fact_sales").fetchone()[0]
        print(f"Before INSERT: fact_sales has {rows_before} rows, total amount = {amt_before} บาท")

        # 2. Add dim_date for 2026-10-01
        cur.execute("""
        INSERT INTO dim_date (date_key, full_date, year, month)
        VALUES (20261001, '2026-10-01', 2026, '2026-10');
        """)

        # 3. Add items to fact_sales:
        # O1007/1: Tea 3 ชิ้น ที่ Chonburi (Bangsaen, store_key=1, product_key=1, unit_price=50)
        # O1007/2: Cookie 2 ชิ้น ที่ Chonburi (Bangsaen, store_key=1, product_key=2, unit_price=80)
        # O1008/1: Tea 4 ชิ้น ที่ Bangkok (Siam, store_key=2, product_key=1, unit_price=50)
        new_items = [
            ('O1007', 1, 20261001, 1, 1, 3, 50),
            ('O1007', 2, 20261001, 2, 1, 2, 80),
            ('O1008', 1, 20261001, 1, 2, 4, 50)
        ]
        cur.executemany("""
        INSERT INTO fact_sales (order_id, line_no, date_key, product_key, store_key, quantity, unit_price)
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """, new_items)
        con.commit()

        # 4. Verify Foreign Keys
        fk_check = cur.execute("PRAGMA foreign_key_check;").fetchall()
        assert len(fk_check) == 0, f"Foreign key check failed: {fk_check}"
        print("PRAGMA foreign_key_check: PASSED (No errors)")

        # 5. Check row count, order count, and revenue before and after JOIN
        chk_q = """
        SELECT
            'fact_sales (before JOIN)' AS source,
            COUNT(*) AS row_count,
            COUNT(DISTINCT order_id) AS order_count,
            SUM(quantity * unit_price) AS total_amount
        FROM fact_sales
        UNION ALL
        SELECT
            'sales (after JOIN)' AS source,
            COUNT(*) AS row_count,
            COUNT(DISTINCT order_id) AS order_count,
            SUM(amount) AS total_amount
        FROM sales;
        """
        df_chk = pd.read_sql_query(chk_q, con)
        print("\n--- Integrity Check (Before vs After JOIN) ---")
        print(df_chk.to_string(index=False))

        # 6. Generate New Pivot Table
        df_sales = pd.read_sql_query("SELECT * FROM sales", con)
        p_new = df_sales.pivot_table(
            index='province',
            columns='month',
            values='amount',
            aggfunc='sum',
            fill_value=0,
            margins=True,
            margins_name='Total'
        )
        print("\n--- New Pivot Table (Province x Month including 2026-10) ---")
        print(p_new)

        # Export challenge pivot to CSV
        csv_path = ROOT / 'pivot_challenge_october.csv'
        p_new.to_csv(csv_path)
        print(f"\nExported challenge pivot table to {csv_path.name}")

if __name__ == '__main__':
    run_challenge_a()
    run_challenge_b()
