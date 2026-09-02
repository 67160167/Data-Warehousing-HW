from pathlib import Path
import json
import pandas as pd
import numpy as np

# Path definitions
DATA = Path(__file__).parent / "data"
OUTPUT = Path(__file__).parent / "output"
OUTPUT.mkdir(exist_ok=True, parents=True)

ROOT_OUTPUT = Path(__file__).parent.parent / "output"
ROOT_OUTPUT.mkdir(exist_ok=True, parents=True)

# Quality Issue Tracking Container
dq_issues = []

# ==============================================================================
# TODO 1: Extract ข้อมูลจาก CSV, Excel และ JSON
# ==============================================================================
print("1. Extracting data from CSV, Excel, and JSON...")
df_o1_raw = pd.read_csv(DATA / "orders_2026_01.csv")
df_o2_raw = pd.read_csv(DATA / "orders_2026_02.csv")
df_c_raw = pd.read_csv(DATA / "customers_crm.csv")
df_p_raw = pd.read_excel(DATA / "product_master.xlsx")
with open(DATA / "payments.json", "r", encoding="utf-8") as f:
    df_pay_raw = pd.json_normalize(json.load(f))

# ==============================================================================
# TODO 2: ทำ schema alignment ของไฟล์ orders สองเดือน แล้ว concat
# ==============================================================================
print("2. Aligning schemas and combining monthly orders...")
df_o2_aligned = df_o2_raw.rename(columns={
    "ordered_at": "order_date",
    "qty": "quantity",
    "discount_pct": "discount"
}).copy()

# Standardize discount & date formats
df_o2_aligned["discount"] = df_o2_aligned["discount"].astype(str).str.rstrip("%").astype(float) / 100.0
df_o1_aligned = df_o1_raw.copy()
df_o1_aligned["order_date"] = pd.to_datetime(df_o1_aligned["order_date"])
df_o2_aligned["order_date"] = pd.to_datetime(df_o2_aligned["order_date"], format="%d/%m/%Y %H:%M")

df_orders_combined = pd.concat([df_o1_aligned, df_o2_aligned], ignore_index=True)

# Track and deduplicate orders (keep='last')
dup_orders = df_orders_combined[df_orders_combined.duplicated(subset=["order_id"], keep=False)]
for oid in dup_orders["order_id"].unique():
    dq_issues.append({
        "stage": "Combine & Deduplicate",
        "dataset": "orders",
        "record_id": oid,
        "column_name": "order_id",
        "issue_type": "DUPLICATE_KEY",
        "issue_description": "Duplicate order_id found. Kept latest record.",
        "action_taken": "DEDUPLICATED (keep=last)"
    })
df_orders_dedup = df_orders_combined.drop_duplicates(subset=["order_id"], keep="last").copy()

# ==============================================================================
# TODO 3: Clean/standardize/deduplicate และสร้าง data quality report
# ==============================================================================
print("3. Cleaning, standardizing, and deduplicating data...")
# Clean Customers
df_c = df_c_raw.copy()
for col in ["customer_id", "full_name", "email", "province", "signup_date"]:
    if col in df_c.columns:
        df_c[col] = df_c[col].astype(str).str.strip()

for idx, r in df_c[df_c["email"].isin(["nan", "none", "", "NaN"]) | df_c["email"].isnull()].iterrows():
    dq_issues.append({
        "stage": "Clean Customers",
        "dataset": "customers",
        "record_id": r["customer_id"],
        "column_name": "email",
        "issue_type": "MISSING_EMAIL",
        "issue_description": "Email address is missing/null.",
        "action_taken": "SET_NULL"
    })
df_c["email"] = df_c["email"].str.lower().replace({"nan": None, "none": None, "": None})

province_map = {
    "ชลบุรี": "ชลบุรี", "Chonburi": "ชลบุรี", "ชลบุรี ": "ชลบุรี",
    "ขอนแก่น": "ขอนแก่น", "ขอนเเก่น": "ขอนแก่น",
    "กรุงเทพมหานคร": "กรุงเทพมหานคร", "Bangkok": "กรุงเทพมหานคร", "กทม.": "กรุงเทพมหานคร",
    "ระยอง": "ระยอง", "Rayong": "ระยอง",
    "ภูเก็ต": "ภูเก็ต", "Phuket": "ภูเก็ต",
    "เชียงใหม่": "เชียงใหม่", "Chiang Mai": "เชียงใหม่"
}

for idx, r in df_c.iterrows():
    raw_p = df_c_raw.loc[idx, "province"]
    std_p = province_map.get(raw_p, raw_p)
    if raw_p != std_p:
        dq_issues.append({
            "stage": "Clean Customers",
            "dataset": "customers",
            "record_id": r["customer_id"],
            "column_name": "province",
            "issue_type": "NON_STANDARD_PROVINCE",
            "issue_description": f"Non-standard province name: '{raw_p}'",
            "action_taken": f"STANDARDIZED to '{std_p}'"
        })
df_c["province"] = df_c["province"].map(province_map).fillna(df_c["province"])

dup_c = df_c[df_c.duplicated(subset=["customer_id"], keep=False)]
for cid in dup_c["customer_id"].unique():
    dq_issues.append({
        "stage": "Clean Customers",
        "dataset": "customers",
        "record_id": cid,
        "column_name": "customer_id",
        "issue_type": "DUPLICATE_KEY",
        "issue_description": f"Duplicate customer_id '{cid}' in CRM.",
        "action_taken": "DEDUPLICATED (keep=last)"
    })
df_c_dim = df_c.drop_duplicates(subset=["customer_id"], keep="last").copy()

# Clean Products
df_p_dim = df_p_raw.copy()
for col in ["product_id", "product_name", "category", "active_flag"]:
    df_p_dim[col] = df_p_dim[col].astype(str).str.strip()

# Clean Payments
df_pay = df_pay_raw.copy()
df_pay = df_pay.rename(columns={"payment.method": "payment_method", "payment.status": "payment_status"})
for col in ["payment_id", "order_id", "payment_method", "payment_status"]:
    if col in df_pay.columns:
        df_pay[col] = df_pay[col].astype(str).str.strip()

dup_pay = df_pay[df_pay.duplicated(subset=["order_id"], keep=False)]
for oid in dup_pay["order_id"].unique():
    dq_issues.append({
        "stage": "Clean Payments",
        "dataset": "payments",
        "record_id": oid,
        "column_name": "order_id",
        "issue_type": "DUPLICATE_KEY",
        "issue_description": f"Duplicate payment record for order_id '{oid}'.",
        "action_taken": "DEDUPLICATED (keep=last)"
    })
df_pay_clean = df_pay.drop_duplicates(subset=["order_id"], keep="last").copy()

# ==============================================================================
# TODO 4: Enrich ด้วย customer, product และ payment master
# ==============================================================================
print("4. Enriching and integrating multi-source datasets...")
m_cust = df_orders_dedup.merge(df_c_dim, on="customer_id", how="left", indicator="_cust_match", validate="m:1")
m_prod = m_cust.merge(df_p_dim, on="product_id", how="left", indicator="_prod_match", validate="m:1")
integrated = m_prod.merge(df_pay_clean, on="order_id", how="left", indicator="_pay_match", validate="1:1")

# ==============================================================================
# TODO 5: Validate business rules ก่อนคำนวณยอดขาย
# ==============================================================================
print("5. Validating business rules and computing Net Sales...")
for idx, r in integrated.iterrows():
    oid = r["order_id"]
    if r["quantity"] <= 0:
        dq_issues.append({
            "stage": "Validate Business Rules",
            "dataset": "orders",
            "record_id": oid,
            "column_name": "quantity",
            "issue_type": "INVALID_QUANTITY",
            "issue_description": f"Quantity <= 0 ({r['quantity']})",
            "action_taken": "EXCLUDED_FROM_FACT_SALES"
        })
    if pd.isnull(r["unit_price"]) or r["unit_price"] <= 0:
        dq_issues.append({
            "stage": "Validate Business Rules",
            "dataset": "orders",
            "record_id": oid,
            "column_name": "unit_price",
            "issue_type": "INVALID_UNIT_PRICE",
            "issue_description": f"Unit price is null or <= 0 ({r['unit_price']})",
            "action_taken": "EXCLUDED_FROM_FACT_SALES"
        })
    if r["discount"] < 0 or r["discount"] > 1:
        dq_issues.append({
            "stage": "Validate Business Rules",
            "dataset": "orders",
            "record_id": oid,
            "column_name": "discount",
            "issue_type": "INVALID_DISCOUNT",
            "issue_description": f"Discount out of range [0, 1] ({r['discount']})",
            "action_taken": "EXCLUDED_FROM_FACT_SALES"
        })
    if r["_cust_match"] != "both":
        dq_issues.append({
            "stage": "Validate Referential Integrity",
            "dataset": "orders",
            "record_id": oid,
            "column_name": "customer_id",
            "issue_type": "UNMATCHED_CUSTOMER_ID",
            "issue_description": f"customer_id '{r['customer_id']}' not in CRM",
            "action_taken": "EXCLUDED_FROM_FACT_SALES"
        })
    if r["_prod_match"] != "both":
        dq_issues.append({
            "stage": "Validate Referential Integrity",
            "dataset": "orders",
            "record_id": oid,
            "column_name": "product_id",
            "issue_type": "UNMATCHED_PRODUCT_ID",
            "issue_description": f"product_id '{r['product_id']}' not in Product Master",
            "action_taken": "EXCLUDED_FROM_FACT_SALES"
        })
    if r["_pay_match"] != "both":
        dq_issues.append({
            "stage": "Validate Payments",
            "dataset": "orders",
            "record_id": oid,
            "column_name": "order_id",
            "issue_type": "UNMATCHED_PAYMENT",
            "issue_description": f"order_id '{oid}' has no payment record",
            "action_taken": "EXCLUDED_FROM_FACT_SALES"
        })
    elif r["payment_status"] != "PAID":
        dq_issues.append({
            "stage": "Validate Business Rules",
            "dataset": "payments",
            "record_id": oid,
            "column_name": "payment_status",
            "issue_type": f"PAYMENT_{r['payment_status']}",
            "issue_description": f"Payment status is {r['payment_status']}",
            "action_taken": "EXCLUDED_FROM_FACT_SALES"
        })

# Filter valid records
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
fact_sales["net_sales"] = fact_sales["quantity"] * fact_sales["unit_price"] * (1.0 - fact_sales["discount"])

# ==============================================================================
# TODO 6: Load dim_customer.csv, dim_product.csv และ fact_sales.csv
# ==============================================================================
print("6. Exporting Dimension and Fact tables...")
dim_customer_out = df_c_dim[["customer_id", "full_name", "email", "province", "signup_date"]].copy()
dim_product_out = df_p_dim[["product_id", "product_name", "category", "standard_price", "active_flag"]].copy()
fact_sales_out = fact_sales[[
    "order_id", "order_date", "customer_id", "product_id",
    "quantity", "unit_price", "discount", "channel",
    "payment_id", "payment_method", "net_sales"
]].copy()
df_dq_report = pd.DataFrame(dq_issues)

# ==============================================================================
# TODO 7: สร้าง summary_by_province.csv และ summary_by_category.csv
# ==============================================================================
print("7. Exporting summary reports...")
summary_prov = fact_sales.groupby("province").agg(
    total_orders=("order_id", "count"),
    total_quantity=("quantity", "sum"),
    total_net_sales=("net_sales", "sum")
).reset_index().sort_values(by="total_net_sales", ascending=False)

summary_cat = fact_sales.groupby("category").agg(
    total_orders=("order_id", "count"),
    total_quantity=("quantity", "sum"),
    total_net_sales=("net_sales", "sum")
).reset_index().sort_values(by="total_net_sales", ascending=False)

for target in [OUTPUT, ROOT_OUTPUT]:
    dim_customer_out.to_csv(target / "dim_customer.csv", index=False, encoding="utf-8-sig")
    dim_product_out.to_csv(target / "dim_product.csv", index=False, encoding="utf-8-sig")
    fact_sales_out.to_csv(target / "fact_sales.csv", index=False, encoding="utf-8-sig")
    df_dq_report.to_csv(target / "data_quality_report.csv", index=False, encoding="utf-8-sig")
    summary_prov.to_csv(target / "summary_by_province.csv", index=False, encoding="utf-8-sig")
    summary_cat.to_csv(target / "summary_by_category.csv", index=False, encoding="utf-8-sig")

print("\nPipeline Complete! All 6 CSV files successfully exported to output/ folder.")
