"""
TechTrove E-Commerce Data Integration Pipeline
----------------------------------------------
Author: Data Engineering Team
Course: Data Warehousing / Data Engineering
Description:
    Extracts raw data from CSV, Excel, and JSON files, aligns schemas, cleans
    and standardizes data, integrates multiple sources with strict validation,
    builds Star Schema dimensional models (dim_customer, dim_product, fact_sales),
    generates a comprehensive Data Quality Report, and produces business summary reports.
"""

from pathlib import Path
import json
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Define paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

# Also ensure root output folder exists if executed from workspace root
ROOT_OUTPUT_DIR = BASE_DIR.parent / "output"
ROOT_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)


def extract_and_profile():
    """Extract raw data from multiple sources and profile initial data quality."""
    print("=" * 60)
    print("STEP 1: EXTRACT & DATA PROFILING")
    print("=" * 60)

    # 1. Orders Jan (CSV)
    df_o1 = pd.read_csv(DATA_DIR / "orders_2026_01.csv")
    # 2. Orders Feb (CSV)
    df_o2 = pd.read_csv(DATA_DIR / "orders_2026_02.csv")
    # 3. Customers CRM (CSV)
    df_c = pd.read_csv(DATA_DIR / "customers_crm.csv")
    # 4. Product Master (Excel)
    df_p = pd.read_excel(DATA_DIR / "product_master.xlsx")
    # 5. Payments (JSON)
    with open(DATA_DIR / "payments.json", "r", encoding="utf-8") as f:
        pay_json = json.load(f)
    df_pay = pd.json_normalize(pay_json)

    raw_datasets = {
        "Orders Jan (orders_2026_01.csv)": df_o1,
        "Orders Feb (orders_2026_02.csv)": df_o2,
        "Customers CRM (customers_crm.csv)": df_c,
        "Product Master (product_master.xlsx)": df_p,
        "Payments Gateway (payments.json)": df_pay,
    }

    profiling_summary = []
    for name, df in raw_datasets.items():
        profiling_summary.append({
            "Dataset": name,
            "Rows": len(df),
            "Columns": len(df.columns),
            "Total Nulls": df.isnull().sum().sum(),
            "Duplicate Rows": df.duplicated().sum(),
            "Column List": ", ".join(df.columns.tolist()),
        })

    df_profile = pd.DataFrame(profiling_summary)
    print(df_profile[["Dataset", "Rows", "Columns", "Total Nulls", "Duplicate Rows"]].to_string(index=False))
    print("\nExtraction & Profiling Completed Successfully.\n")

    return df_o1, df_o2, df_c, df_p, df_pay


def align_and_combine_orders(df_o1, df_o2, dq_issues):
    """Align schema differences between monthly orders and concatenate."""
    print("=" * 60)
    print("STEP 2: SCHEMA ALIGNMENT & COMBINE ORDERS")
    print("=" * 60)

    # Align column names for February orders
    df_o2_aligned = df_o2.rename(columns={
        "ordered_at": "order_date",
        "qty": "quantity",
        "discount_pct": "discount",
    }).copy()

    # Standardize discount format: Feb has '5%', '10%' string format -> convert to float 0.05, 0.10
    df_o2_aligned["discount"] = (
        df_o2_aligned["discount"]
        .astype(str)
        .str.rstrip("%")
        .astype(float)
        / 100.0
    )

    # Standardize datetime formats
    df_o1_aligned = df_o1.copy()
    df_o1_aligned["order_date"] = pd.to_datetime(df_o1_aligned["order_date"])
    df_o2_aligned["order_date"] = pd.to_datetime(
        df_o2_aligned["order_date"], format="%d/%m/%Y %H:%M"
    )

    # Concat orders
    df_orders_combined = pd.concat([df_o1_aligned, df_o2_aligned], ignore_index=True)
    print(f"Total Combined Orders: {len(df_orders_combined)} rows (Jan: {len(df_o1)} + Feb: {len(df_o2)})")

    # Deduplicate orders: keep latest occurrence per business rule
    dup_mask = df_orders_combined.duplicated(subset=["order_id"], keep=False)
    dup_orders = df_orders_combined[dup_mask]
    for oid in dup_orders["order_id"].unique():
        rows = dup_orders[dup_orders["order_id"] == oid]
        dq_issues.append({
            "stage": "2. Combine & Deduplicate",
            "dataset": "orders",
            "record_id": oid,
            "column_name": "order_id",
            "issue_type": "DUPLICATE_KEY",
            "issue_description": f"Duplicate order_id found ({len(rows)} occurrences). Kept latest record.",
            "action_taken": "DEDUPLICATED (keep=last)",
        })

    df_orders_dedup = df_orders_combined.drop_duplicates(subset=["order_id"], keep="last").copy()
    print(f"Orders after Deduplication (keep='last'): {len(df_orders_dedup)} rows (Removed {len(df_orders_combined) - len(df_orders_dedup)} duplicates)\n")

    return df_orders_dedup


def clean_and_standardize(df_c_raw, df_p_raw, df_pay_raw, dq_issues):
    """Clean and standardize dimensions and payment gateway data."""
    print("=" * 60)
    print("STEP 3: DATA CLEANING & STANDARDIZATION")
    print("=" * 60)

    # --- 3.1 Customers Cleaning ---
    df_c = df_c_raw.copy()
    for col in ["customer_id", "full_name", "email", "province", "signup_date"]:
        if col in df_c.columns:
            df_c[col] = df_c[col].astype(str).str.strip()

    # Track missing emails
    for idx, r in df_c[df_c["email"].isin(["nan", "none", "", "NaN"]) | df_c["email"].isnull()].iterrows():
        dq_issues.append({
            "stage": "3. Clean Customers",
            "dataset": "customers",
            "record_id": r["customer_id"],
            "column_name": "email",
            "issue_type": "MISSING_EMAIL",
            "issue_description": "Customer email address is missing/null.",
            "action_taken": "SET_NULL",
        })

    df_c["email"] = df_c["email"].str.lower().replace({"nan": None, "none": None, "": None})

    # Province Standardization Map
    province_map = {
        "ชลบุรี": "ชลบุรี",
        "Chonburi": "ชลบุรี",
        "ชลบุรี ": "ชลบุรี",
        "ขอนแก่น": "ขอนแก่น",
        "ขอนเเก่น": "ขอนแก่น",
        "กรุงเทพมหานคร": "กรุงเทพมหานคร",
        "Bangkok": "กรุงเทพมหานคร",
        "กทม.": "กรุงเทพมหานคร",
        "ระยอง": "ระยอง",
        "Rayong": "ระยอง",
        "ภูเก็ต": "ภูเก็ต",
        "Phuket": "ภูเก็ต",
        "เชียงใหม่": "เชียงใหม่",
        "Chiang Mai": "เชียงใหม่",
    }

    for idx, r in df_c.iterrows():
        raw_p = df_c_raw.loc[idx, "province"]
        std_p = province_map.get(raw_p, raw_p)
        if raw_p != std_p:
            dq_issues.append({
                "stage": "3. Clean Customers",
                "dataset": "customers",
                "record_id": r["customer_id"],
                "column_name": "province",
                "issue_type": "NON_STANDARD_PROVINCE",
                "issue_description": f"Non-standard province name: '{raw_p}'",
                "action_taken": f"STANDARDIZED to '{std_p}'",
            })

    df_c["province"] = df_c["province"].map(province_map).fillna(df_c["province"])

    # Deduplicate customers (keep='last')
    dup_c = df_c[df_c.duplicated(subset=["customer_id"], keep=False)]
    for cid in dup_c["customer_id"].unique():
        dq_issues.append({
            "stage": "3. Clean Customers",
            "dataset": "customers",
            "record_id": cid,
            "column_name": "customer_id",
            "issue_type": "DUPLICATE_KEY",
            "issue_description": f"Duplicate customer_id '{cid}' in CRM.",
            "action_taken": "DEDUPLICATED (keep=last)",
        })

    df_c_dim = df_c.drop_duplicates(subset=["customer_id"], keep="last").copy()
    print(f"Customer Master Cleaned: {len(df_c_dim)} unique customers (from {len(df_c_raw)} raw rows).")

    # --- 3.2 Product Master Cleaning ---
    df_p_dim = df_p_raw.copy()
    for col in ["product_id", "product_name", "category", "active_flag"]:
        df_p_dim[col] = df_p_dim[col].astype(str).str.strip()
    print(f"Product Master Cleaned: {len(df_p_dim)} products.")

    # --- 3.3 Payments Cleaning ---
    df_pay = df_pay_raw.copy()
    df_pay = df_pay.rename(columns={
        "payment.method": "payment_method",
        "payment.status": "payment_status",
    })
    for col in ["payment_id", "order_id", "payment_method", "payment_status"]:
        if col in df_pay.columns:
            df_pay[col] = df_pay[col].astype(str).str.strip()

    dup_pay = df_pay[df_pay.duplicated(subset=["order_id"], keep=False)]
    for oid in dup_pay["order_id"].unique():
        dq_issues.append({
            "stage": "3. Clean Payments",
            "dataset": "payments",
            "record_id": oid,
            "column_name": "order_id",
            "issue_type": "DUPLICATE_KEY",
            "issue_description": f"Duplicate payment record for order_id '{oid}'.",
            "action_taken": "DEDUPLICATED (keep=last)",
        })
    df_pay_clean = df_pay.drop_duplicates(subset=["order_id"], keep="last").copy()
    print(f"Payments Cleaned: {len(df_pay_clean)} unique payment events.\n")

    return df_c_dim, df_p_dim, df_pay_clean


def integrate_and_validate(df_orders_dedup, df_c_dim, df_p_dim, df_pay_clean, dq_issues):
    """Integrate orders with customer, product, and payments data with strict validation."""
    print("=" * 60)
    print("STEP 4: INTEGRATION & VALIDATION")
    print("=" * 60)

    # 4.1 Merge with Customer Master (Many-to-One)
    m_cust = df_orders_dedup.merge(
        df_c_dim,
        on="customer_id",
        how="left",
        indicator="_cust_match",
        validate="m:1",
    )

    # 4.2 Merge with Product Master (Many-to-One)
    m_prod = m_cust.merge(
        df_p_dim,
        on="product_id",
        how="left",
        indicator="_prod_match",
        validate="m:1",
    )

    # 4.3 Merge with Payments (One-to-One)
    integrated = m_prod.merge(
        df_pay_clean,
        on="order_id",
        how="left",
        indicator="_pay_match",
        validate="1:1",
    )

    print(f"Integrated Orders count: {len(integrated)} rows")
    print(f"Customer match: both={sum(integrated['_cust_match'] == 'both')}, unmatched={sum(integrated['_cust_match'] != 'both')}")
    print(f"Product match: both={sum(integrated['_prod_match'] == 'both')}, unmatched={sum(integrated['_prod_match'] != 'both')}")
    print(f"Payment match: both={sum(integrated['_pay_match'] == 'both')}, unmatched={sum(integrated['_pay_match'] != 'both')}")

    # Track all order validation anomalies
    for idx, r in integrated.iterrows():
        oid = r["order_id"]
        # Rule 1: quantity > 0
        if r["quantity"] <= 0:
            dq_issues.append({
                "stage": "4. Validate Business Rules",
                "dataset": "orders",
                "record_id": oid,
                "column_name": "quantity",
                "issue_type": "INVALID_QUANTITY",
                "issue_description": f"Quantity <= 0 (value: {r['quantity']})",
                "action_taken": "EXCLUDED_FROM_FACT_SALES",
            })
        # Rule 2: unit_price > 0 and not null
        if pd.isnull(r["unit_price"]) or r["unit_price"] <= 0:
            dq_issues.append({
                "stage": "4. Validate Business Rules",
                "dataset": "orders",
                "record_id": oid,
                "column_name": "unit_price",
                "issue_type": "INVALID_UNIT_PRICE",
                "issue_description": f"Unit price is null or <= 0 (value: {r['unit_price']})",
                "action_taken": "EXCLUDED_FROM_FACT_SALES",
            })
        # Rule 3: discount in [0, 1]
        if r["discount"] < 0 or r["discount"] > 1:
            dq_issues.append({
                "stage": "4. Validate Business Rules",
                "dataset": "orders",
                "record_id": oid,
                "column_name": "discount",
                "issue_type": "INVALID_DISCOUNT",
                "issue_description": f"Discount out of range [0, 1] (value: {r['discount']})",
                "action_taken": "EXCLUDED_FROM_FACT_SALES",
            })
        # Rule 4: Referential integrity Customer
        if r["_cust_match"] != "both":
            dq_issues.append({
                "stage": "4. Validate Referential Integrity",
                "dataset": "orders",
                "record_id": oid,
                "column_name": "customer_id",
                "issue_type": "UNMATCHED_CUSTOMER_ID",
                "issue_description": f"customer_id '{r['customer_id']}' not found in Customer Master",
                "action_taken": "EXCLUDED_FROM_FACT_SALES",
            })
        # Rule 5: Referential integrity Product
        if r["_prod_match"] != "both":
            dq_issues.append({
                "stage": "4. Validate Referential Integrity",
                "dataset": "orders",
                "record_id": oid,
                "column_name": "product_id",
                "issue_type": "UNMATCHED_PRODUCT_ID",
                "issue_description": f"product_id '{r['product_id']}' not found in Product Master",
                "action_taken": "EXCLUDED_FROM_FACT_SALES",
            })
        # Rule 6: Payment matched and status == PAID
        if r["_pay_match"] != "both":
            dq_issues.append({
                "stage": "4. Validate Payments",
                "dataset": "orders",
                "record_id": oid,
                "column_name": "order_id",
                "issue_type": "UNMATCHED_PAYMENT",
                "issue_description": f"order_id '{oid}' has no matching payment record",
                "action_taken": "EXCLUDED_FROM_FACT_SALES",
            })
        elif r["payment_status"] != "PAID":
            dq_issues.append({
                "stage": "4. Validate Business Rules",
                "dataset": "payments",
                "record_id": oid,
                "column_name": "payment_status",
                "issue_type": f"PAYMENT_{r['payment_status']}",
                "issue_description": f"Payment status is {r['payment_status']} (not PAID)",
                "action_taken": "EXCLUDED_FROM_FACT_SALES",
            })

    # Filter strictly valid sales transactions
    valid_mask = (
        (integrated["quantity"] > 0)
        & (integrated["unit_price"].notnull())
        & (integrated["unit_price"] > 0)
        & (integrated["discount"] >= 0)
        & (integrated["discount"] <= 1)
        & (integrated["_cust_match"] == "both")
        & (integrated["_prod_match"] == "both")
        & (integrated["_pay_match"] == "both")
        & (integrated["payment_status"] == "PAID")
    )

    fact_sales = integrated[valid_mask].copy()

    # Calculate net_sales: quantity * unit_price * (1 - discount)
    fact_sales["net_sales"] = (
        fact_sales["quantity"]
        * fact_sales["unit_price"]
        * (1.0 - fact_sales["discount"])
    )

    print(f"\nFiltered Valid Sales Transactions: {len(fact_sales)} rows")
    print(f"Total Excluded Orders: {len(integrated) - len(fact_sales)} rows")
    print(f"Total Net Sales: {fact_sales['net_sales'].sum():,.2f} THB\n")

    return fact_sales, integrated


def validate_data(df_fact, df_cust, df_prod):
    """
    Challenge Function (+2 Points):
    Validates data integrity with assertions / exceptions.
    Checks:
        1. Uniqueness of Primary Keys
        2. Referential Integrity between Fact and Dimensions
        3. Business Value Domain & Range Constraints
        4. Completeness (No Null values in critical fields)
    """
    print("=" * 60)
    print("CHALLENGE: VALIDATING DATA INTEGRITY (ASSERTIONS)")
    print("=" * 60)

    # 1. Uniqueness Checks
    assert df_fact["order_id"].is_unique, "Assertion Failed: Fact table order_id must be unique!"
    assert df_cust["customer_id"].is_unique, "Assertion Failed: Dimension Customer customer_id must be unique!"
    assert df_prod["product_id"].is_unique, "Assertion Failed: Dimension Product product_id must be unique!"
    print("[PASS] Assertion 1: Uniqueness of Primary Keys validated.")

    # 2. Referential Integrity Checks
    invalid_cust = ~df_fact["customer_id"].isin(df_cust["customer_id"])
    assert invalid_cust.sum() == 0, f"Assertion Failed: {invalid_cust.sum()} orphan customer_ids in Fact Sales!"
    invalid_prod = ~df_fact["product_id"].isin(df_prod["product_id"])
    assert invalid_prod.sum() == 0, f"Assertion Failed: {invalid_prod.sum()} orphan product_ids in Fact Sales!"
    print("[PASS] Assertion 2: Referential Integrity (Foreign Keys) validated.")

    # 3. Domain / Range Constraints Checks
    assert (df_fact["quantity"] > 0).all(), "Assertion Failed: All quantities must be > 0!"
    assert (df_fact["unit_price"] > 0).all(), "Assertion Failed: All unit prices must be > 0!"
    assert ((df_fact["discount"] >= 0) & (df_fact["discount"] <= 1)).all(), "Assertion Failed: Discount must be between 0 and 1!"
    assert (df_fact["net_sales"] >= 0).all(), "Assertion Failed: Net Sales must be non-negative!"
    print("[PASS] Assertion 3: Value domain and range constraints validated.")

    # 4. Completeness Checks
    critical_cols = ["order_id", "order_date", "customer_id", "product_id", "quantity", "unit_price", "discount", "net_sales"]
    for col in critical_cols:
        assert df_fact[col].notnull().all(), f"Assertion Failed: Column '{col}' in Fact Sales contains nulls!"
    print("[PASS] Assertion 4: Completeness (zero nulls in critical columns) validated.")
    print("All Data Quality Assertions Passed 100% Successfully!\n")


def build_and_export_outputs(df_c_dim, df_p_dim, fact_sales, dq_issues):
    """Generate final output files and summaries."""
    print("=" * 60)
    print("STEP 5: LOAD DIMENSIONS, FACT, AND SUMMARY REPORTS")
    print("=" * 60)

    # 1. dim_customer.csv
    dim_customer_out = df_c_dim[["customer_id", "full_name", "email", "province", "signup_date"]].copy()

    # 2. dim_product.csv
    dim_product_out = df_p_dim[["product_id", "product_name", "category", "standard_price", "active_flag"]].copy()

    # 3. fact_sales.csv
    fact_sales_out = fact_sales[[
        "order_id",
        "order_date",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount",
        "channel",
        "payment_id",
        "payment_method",
        "net_sales",
    ]].copy()

    # 4. data_quality_report.csv
    df_dq_report = pd.DataFrame(dq_issues)

    # 5. summary_by_province.csv
    summary_prov = fact_sales.groupby("province").agg(
        total_orders=("order_id", "count"),
        total_quantity=("quantity", "sum"),
        total_net_sales=("net_sales", "sum"),
    ).reset_index().sort_values(by="total_net_sales", ascending=False)

    # 6. summary_by_category.csv
    summary_cat = fact_sales.groupby("category").agg(
        total_orders=("order_id", "count"),
        total_quantity=("quantity", "sum"),
        total_net_sales=("net_sales", "sum"),
    ).reset_index().sort_values(by="total_net_sales", ascending=False)

    # Export to both OUTPUT_DIR and ROOT_OUTPUT_DIR
    target_dirs = [OUTPUT_DIR, ROOT_OUTPUT_DIR]
    for target in target_dirs:
        dim_customer_out.to_csv(target / "dim_customer.csv", index=False, encoding="utf-8-sig")
        dim_product_out.to_csv(target / "dim_product.csv", index=False, encoding="utf-8-sig")
        fact_sales_out.to_csv(target / "fact_sales.csv", index=False, encoding="utf-8-sig")
        df_dq_report.to_csv(target / "data_quality_report.csv", index=False, encoding="utf-8-sig")
        summary_prov.to_csv(target / "summary_by_province.csv", index=False, encoding="utf-8-sig")
        summary_cat.to_csv(target / "summary_by_category.csv", index=False, encoding="utf-8-sig")

    print("Generated 6 Output Files Successfully:")
    print(f"  1. dim_customer.csv         ({len(dim_customer_out)} rows)")
    print(f"  2. dim_product.csv          ({len(dim_product_out)} rows)")
    print(f"  3. fact_sales.csv           ({len(fact_sales_out)} rows)")
    print(f"  4. data_quality_report.csv  ({len(df_dq_report)} issues logged)")
    print(f"  5. summary_by_province.csv  ({len(summary_prov)} provinces)")
    print(f"  6. summary_by_category.csv  ({len(summary_cat)} categories)\n")

    return summary_prov, summary_cat, df_dq_report


def create_quality_funnel_chart(raw_orders_count, dedup_orders_count, matched_orders_count, paid_sales_count):
    """Generate Data Quality Funnel Visualization Chart."""
    stages = [
        "1. Raw Orders\n(Jan + Feb)",
        "2. Deduplicated\nOrders",
        "3. Valid Master\n& Price/Qty",
        "4. Fully Paid Sales\n(Fact Sales)",
    ]
    counts = [raw_orders_count, dedup_orders_count, matched_orders_count, paid_sales_count]
    retention_pct = [c / raw_orders_count * 100 for c in counts]

    fig, ax = plt.subplots(figsize=(10, 6), dpi=300)
    colors = ["#4A90E2", "#50E3C2", "#F5A623", "#7ED321"]

    bars = ax.bar(stages, counts, color=colors, edgecolor="#2C3E50", width=0.55, linewidth=1.5)

    for bar, count, pct in zip(bars, counts, retention_pct):
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2.0,
            height + 15,
            f"{count:,} rows\n({pct:.1f}%)",
            ha="center",
            va="bottom",
            fontsize=11,
            fontweight="bold",
            color="#2C3E50",
        )

    ax.set_ylim(0, max(counts) * 1.22)
    ax.set_title("TechTrove Data Quality Pipeline Funnel", fontsize=16, fontweight="bold", pad=20)
    ax.set_ylabel("Record Count", fontsize=12, fontweight="bold")
    ax.grid(axis="y", linestyle="--", alpha=0.5)

    plt.tight_layout()
    chart_path = OUTPUT_DIR / "data_quality_funnel.png"
    plt.savefig(chart_path)
    plt.savefig(ROOT_OUTPUT_DIR / "data_quality_funnel.png")
    plt.close()
    print(f"Data Quality Funnel Chart saved to {chart_path}\n")


def print_analytical_answers(df_orders_combined, df_orders_dedup, integrated, fact_sales, summary_prov, summary_cat):
    """Print the exact answers to the 6 business analysis questions."""
    print("=" * 60)
    print("ANALYSIS & BUSINESS QUESTIONS (คำตอบคำถามวิเคราะห์ 6 ข้อ)")
    print("=" * 60)

    # Q1
    q1_combined = len(df_orders_combined)
    q1_dedup = len(df_orders_dedup)
    q1_dropped = q1_combined - q1_dedup
    print(f"ข้อ 1: หลังรวมไฟล์ orders มีจำนวนแถวเท่าใด และเหลือกี่แถวหลังลบ duplicate?")
    print(f"  -> รวมไฟล์ orders ม.ค. (361) และ ก.พ. (391) ได้ทั้งหมด {q1_combined} แถว")
    print(f"  -> เหลือกี่แถวหลังลบ duplicate: เหลือ {q1_dedup} แถว (ลบข้อมูลซ้ำออก {q1_dropped} แถว โดยเก็บข้อมูลล่าสุด keep='last')\n")

    # Q2
    unmatched_cust = sum(integrated["_cust_match"] != "both")
    unmatched_prod = sum(integrated["_prod_match"] != "both")
    print(f"ข้อ 2: มีแถวที่ customer_id หรือ product_id ไม่พบใน Master Data อย่างละกี่แถว?")
    print(f"  -> customer_id ไม่พบใน Master Data: {unmatched_cust} แถว (รหัสที่พบปัญหา: C0161-C0165)")
    print(f"  -> product_id ไม่พบใน Master Data: {unmatched_prod} แถว (รหัสที่พบปัญหา: P999)\n")

    # Q3
    valid_tx = len(fact_sales)
    total_net = fact_sales["net_sales"].sum()
    print(f"ข้อ 3: มียอดขายที่ใช้ได้จริงกี่ธุรกรรม และยอดขายสุทธิรวมเท่าใด?")
    print(f"  -> ยอดขายที่ใช้ได้จริง (Fact Sales): {valid_tx:,} ธุรกรรม")
    print(f"  -> ยอดขายสุทธิรวม (Total Net Sales): {total_net:,.2f} บาท\n")

    # Q4
    top_prov = summary_prov.iloc[0]
    print(f"ข้อ 4: จังหวัดใดมียอดขายสุทธิสูงสุด?")
    print(f"  -> จังหวัด: {top_prov['province']} มียอดขายสุทธิสูงสุด {top_prov['total_net_sales']:,.2f} บาท ({top_prov['total_orders']} ออเดอร์)\n")

    # Q5
    top_cat = summary_cat.iloc[0]
    print(f"ข้อ 5: หมวดสินค้าใดมียอดขายสุทธิสูงสุด?")
    print(f"  -> หมวดสินค้า: {top_cat['category']} มียอดขายสุทธิสูงสุด {top_cat['total_net_sales']:,.2f} บาท ({top_cat['total_orders']} ออเดอร์)\n")

    # Q6
    print(f"ข้อ 6: หากสลับลำดับ merge ก่อน cleaning ผลลัพธ์หรือความเชื่อมั่นของข้อมูลเปลี่ยนอย่างไร?")
    print("  -> ผลกระทบและความเสี่ยงเมื่อ Merge ก่อน Clean:")
    print("     1) Cardinality Explosion / Cartesian Product จาก Duplicate:")
    print("        หากไม่ Deduplicate Customer/Payment ก่อน Merge แบบ Many-to-One / One-to-One จะเกิดแถวซ้ำงอกออกมา (Duplicate Multiplier) ทำให้ยอดขายและจำนวนนับบวมเกินจริง")
    print("     2) Referential Integrity Failure จากข้อมูลไม่ Standardize:")
    print("        หากไม่ Clean ช่องว่าง (Whitespace Trimming) และ Case-sensitivity ข้อมูลที่มี space นำหน้า/ตามหลังจะไม่ Match (Unmatched Join Loss)")
    print("     3) Invalid Business Computation:")
    print("        หากคำนวณ net_sales ก่อน Clean ชนิดข้อมูล (เช่น Discount เป็น string '5%' หรือ unit_price มี NaN/ค่าลบ) จะเกิด Type Error หรือคำนวณยอดขายผิดพลาด")
    print("     4) Loss of Data Quality Lineage:")
    print("        การ Clean และ Validate เป็นขั้นตอนทำให้บันทึก Root Cause ใน Data Quality Report ได้อย่างโปร่งใส ตรวจสอบย้อนกลับได้ (Auditability)")
    print("=" * 60)


def main():
    dq_issues = []

    # Step 1: Extract & Profile
    df_o1, df_o2, df_c, df_p, df_pay = extract_and_profile()

    # Step 2: Combine Orders
    df_orders_dedup = align_and_combine_orders(df_o1, df_o2, dq_issues)

    # Step 3: Clean & Standardize
    df_c_dim, df_p_dim, df_pay_clean = clean_and_standardize(df_c, df_p, df_pay, dq_issues)

    # Step 4: Integrate & Validate
    fact_sales, integrated = integrate_and_validate(df_orders_dedup, df_c_dim, df_p_dim, df_pay_clean, dq_issues)

    # Challenge Step: Validate Data with Assertions
    validate_data(fact_sales, df_c_dim, df_p_dim)

    # Step 5: Load Outputs
    summary_prov, summary_cat, df_dq_report = build_and_export_outputs(df_c_dim, df_p_dim, fact_sales, dq_issues)

    # Matched stage count for funnel (Orders that passed schema, dedup, master lookup, price/qty checks)
    matched_and_valid = integrated[
        (integrated["quantity"] > 0)
        & (integrated["unit_price"].notnull())
        & (integrated["unit_price"] > 0)
        & (integrated["discount"] >= 0)
        & (integrated["discount"] <= 1)
        & (integrated["_cust_match"] == "both")
        & (integrated["_prod_match"] == "both")
    ]

    # Generate Visualization Chart
    create_quality_funnel_chart(
        raw_orders_count=len(df_o1) + len(df_o2),
        dedup_orders_count=len(df_orders_dedup),
        matched_orders_count=len(matched_and_valid),
        paid_sales_count=len(fact_sales),
    )

    # Step 6: Print Analytical Answers
    df_orders_combined = pd.concat([df_o1, df_o2], ignore_index=True)
    print_analytical_answers(df_orders_combined, df_orders_dedup, integrated, fact_sales, summary_prov, summary_cat)


if __name__ == "__main__":
    main()
