"""
Python Data Pipeline Engineering - Omnichannel Retail Data Warehouse
Incremental & Idempotent ETL Pipeline
"""

import os
import sys
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# Task 1: Pipeline Configuration & Logging Setup
# ---------------------------------------------------------------------------

@dataclass
class PipelineConfig:
    dataset_path: str = "Python_Data_Pipeline_Lab_Dataset (1).xlsx"
    db_path: str = "retail_dw.db"
    quarantine_path: str = "quarantine.csv"
    log_path: str = "pipeline_run_log.csv"
    batch_list: List[str] = field(default_factory=lambda: [
        "orders_batch_1",
        "orders_batch_1",  # Run again for idempotency test
        "orders_batch_2",
        "orders_batch_3"
    ])
    error_mode: str = "quarantine"  # 'quarantine' or 'fail'


def setup_logger() -> logging.Logger:
    """Configures structured logger for pipeline execution."""
    logger = logging.getLogger("DataPipeline")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

logger = setup_logger()


# ---------------------------------------------------------------------------
# Task 3: Database & Star Schema DDL Initialization
# ---------------------------------------------------------------------------

def init_database(db_path: str):
    """
    Initializes SQLite Database schema:
    - dim_customer
    - dim_product
    - dim_date
    - fact_sales
    - pipeline_run_log
    - quarantine_records
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    cursor = conn.cursor()

    cursor.executescript("""
    -- Customer Dimension
    CREATE TABLE IF NOT EXISTS dim_customer (
        customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id TEXT UNIQUE NOT NULL,
        customer_name TEXT,
        province TEXT,
        segment TEXT
    );

    -- Product Dimension
    CREATE TABLE IF NOT EXISTS dim_product (
        product_key INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id TEXT UNIQUE NOT NULL,
        product_name TEXT,
        category TEXT
    );

    -- Date Dimension
    CREATE TABLE IF NOT EXISTS dim_date (
        date_key INTEGER PRIMARY KEY,
        full_date TEXT UNIQUE NOT NULL,
        day INTEGER NOT NULL,
        month INTEGER NOT NULL,
        quarter INTEGER NOT NULL,
        year INTEGER NOT NULL
    );

    -- Fact Sales Table (Grain: 1 validated order line per order_id)
    CREATE TABLE IF NOT EXISTS fact_sales (
        order_id TEXT PRIMARY KEY,
        date_key INTEGER NOT NULL,
        customer_key INTEGER NOT NULL,
        product_key INTEGER NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price REAL NOT NULL,
        discount_pct REAL NOT NULL,
        gross_amount REAL NOT NULL,
        net_amount REAL NOT NULL,
        payment_method TEXT NOT NULL,
        sales_channel TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
        FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
        FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
    );

    -- Pipeline Run Log / Watermark Table
    CREATE TABLE IF NOT EXISTS pipeline_run_log (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_name TEXT NOT NULL,
        started_at TEXT NOT NULL,
        ended_at TEXT NOT NULL,
        rows_read INTEGER NOT NULL,
        rows_valid INTEGER NOT NULL,
        rows_rejected INTEGER NOT NULL,
        rows_duplicated INTEGER NOT NULL,
        rows_loaded INTEGER NOT NULL,
        net_sales_loaded REAL NOT NULL,
        status TEXT NOT NULL,
        error_message TEXT
    );

    -- Quarantine Records Table
    CREATE TABLE IF NOT EXISTS quarantine_records (
        quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_batch TEXT NOT NULL,
        order_id TEXT,
        order_datetime TEXT,
        customer_id TEXT,
        product_id TEXT,
        quantity TEXT,
        unit_price TEXT,
        discount_pct TEXT,
        payment_method TEXT,
        sales_channel TEXT,
        updated_at TEXT,
        reason_code TEXT NOT NULL,
        quarantined_at TEXT NOT NULL
    );
    """)

    conn.commit()
    conn.close()
    logger.info(f"Initialized SQLite Star Schema Database at '{db_path}'")


# ---------------------------------------------------------------------------
# Task 1 & 3: Dimension Loading (Customers, Products, Dates)
# ---------------------------------------------------------------------------

def load_dimensions(dataset_path: str, db_path: str):
    """Extracts customer & product dimension tables from Excel and loads them into SQLite."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Load Customers
    try:
        df_cust = pd.read_excel(dataset_path, sheet_name="customers")
        cust_records = [
            (
                str(row['customer_id']).strip(),
                str(row['customer_name']).strip() if pd.notna(row['customer_name']) else None,
                str(row['province']).strip() if pd.notna(row['province']) else None,
                str(row['segment']).strip() if pd.notna(row['segment']) else None
            )
            for _, row in df_cust.iterrows()
            if pd.notna(row['customer_id'])
        ]
        cursor.executemany("""
            INSERT OR IGNORE INTO dim_customer (customer_id, customer_name, province, segment)
            VALUES (?, ?, ?, ?);
        """, cust_records)
        logger.info(f"Loaded {len(cust_records)} records into dim_customer")
    except Exception as e:
        logger.error(f"Error loading dim_customer: {e}")
        conn.close()
        raise

    # Load Products
    try:
        df_prod = pd.read_excel(dataset_path, sheet_name="products")
        prod_records = [
            (
                str(row['product_id']).strip(),
                str(row['product_name']).strip() if pd.notna(row['product_name']) else None,
                str(row['category']).strip() if pd.notna(row['category']) else None
            )
            for _, row in df_prod.iterrows()
            if pd.notna(row['product_id'])
        ]
        cursor.executemany("""
            INSERT OR IGNORE INTO dim_product (product_id, product_name, category)
            VALUES (?, ?, ?);
        """, prod_records)
        logger.info(f"Loaded {len(prod_records)} records into dim_product")
    except Exception as e:
        logger.error(f"Error loading dim_product: {e}")
        conn.close()
        raise

    conn.commit()
    conn.close()


def ensure_date_dimension(dates: List[pd.Timestamp], db_path: str):
    """Populates dim_date table for all transaction dates."""
    if not dates:
        return
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    date_records = []
    for dt in dates:
        date_key = int(dt.strftime("%Y%m%d"))
        full_date = dt.strftime("%Y-%m-%d")
        day = dt.day
        month = dt.month
        quarter = (dt.month - 1) // 3 + 1
        year = dt.year
        date_records.append((date_key, full_date, day, month, quarter, year))

    cursor.executemany("""
        INSERT OR IGNORE INTO dim_date (date_key, full_date, day, month, quarter, year)
        VALUES (?, ?, ?, ?, ?, ?);
    """, date_records)

    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Task 2: Data Normalization & Transformation Mappings
# ---------------------------------------------------------------------------

PAYMENT_METHOD_MAP = {
    'cash': 'Cash',
    'bank transfer': 'Bank Transfer',
    'promptpay': 'PromptPay',
    'credit card': 'Credit Card'
}

SALES_CHANNEL_MAP = {
    'e-commerce': 'Online',
    'online': 'Online',
    'store': 'Store',
    'marketplace': 'Marketplace'
}


def normalize_payment_method(val) -> str:
    """Normalizes payment_method case and approved labels."""
    if pd.isna(val):
        return "Unknown"
    cleaned = str(val).strip().lower()
    return PAYMENT_METHOD_MAP.get(cleaned, str(val).strip())


def normalize_sales_channel(val) -> str:
    """Maps E-Commerce to Online and standardizes sales channel."""
    if pd.isna(val):
        return "Unknown"
    cleaned = str(val).strip().lower()
    return SALES_CHANNEL_MAP.get(cleaned, str(val).strip())


# ---------------------------------------------------------------------------
# Task 2: Transform, Validation & Data Quality (DQ) Engine
# ---------------------------------------------------------------------------

def transform_and_validate_batch(
    df: pd.DataFrame,
    source_batch: str,
    valid_customer_ids: set,
    valid_product_ids: set
) -> Tuple[pd.DataFrame, pd.DataFrame, int]:
    """
    Executes Data Quality validations, normalizations, calculations,
    and deduplication on an orders batch.

    Returns:
        (clean_df, quarantine_df, num_duplicates_removed)
    """
    reasons = []
    is_valid_row = []

    # Safe conversions
    parsed_order_dts = pd.to_datetime(df['order_datetime'], errors='coerce')
    parsed_updated_dts = pd.to_datetime(df['updated_at'], errors='coerce')
    parsed_quantities = pd.to_numeric(df['quantity'], errors='coerce')
    parsed_prices = pd.to_numeric(df['unit_price'], errors='coerce')
    parsed_discounts = pd.to_numeric(df['discount_pct'], errors='coerce')

    for idx in range(len(df)):
        row_reasons = []

        # 1. Datetime check
        if pd.isna(parsed_order_dts.iloc[idx]):
            row_reasons.append("INVALID_DATETIME")
        if pd.isna(parsed_updated_dts.iloc[idx]):
            row_reasons.append("INVALID_UPDATED_AT")

        # 2. Customer Foreign Key check
        cust_id = df['customer_id'].iloc[idx]
        if pd.isna(cust_id) or str(cust_id).strip() not in valid_customer_ids:
            row_reasons.append("UNKNOWN_CUSTOMER")

        # 3. Product Foreign Key check
        prod_id = df['product_id'].iloc[idx]
        if pd.isna(prod_id) or str(prod_id).strip() not in valid_product_ids:
            row_reasons.append("UNKNOWN_PRODUCT")

        # 4. Quantity rule (Integer between 1 and 20)
        qty = parsed_quantities.iloc[idx]
        if pd.isna(qty) or qty <= 0 or qty > 20 or (qty % 1 != 0):
            row_reasons.append("INVALID_QUANTITY")

        # 5. Price rule (Numeric and > 0)
        price = parsed_prices.iloc[idx]
        if pd.isna(price) or price <= 0:
            row_reasons.append("INVALID_UNIT_PRICE")

        # 6. Discount percent rule (0 to 100)
        disc = parsed_discounts.iloc[idx]
        if pd.isna(disc) or disc < 0 or disc > 100:
            row_reasons.append("INVALID_DISCOUNT_PCT")

        # Result for row
        if row_reasons:
            is_valid_row.append(False)
            reasons.append("; ".join(row_reasons))
        else:
            is_valid_row.append(True)
            reasons.append("")

    is_valid_series = pd.Series(is_valid_row, index=df.index)

    # 1. Quarantine DataFrame
    df_quarantine = df[~is_valid_series].copy()
    if not df_quarantine.empty:
        df_quarantine['reason_code'] = [reasons[i] for i in range(len(df)) if not is_valid_series.iloc[i]]
        df_quarantine['source_batch'] = source_batch
        df_quarantine['quarantined_at'] = datetime.now().isoformat()

    # 2. Clean DataFrame
    df_valid = df[is_valid_series].copy()
    num_duplicates_removed = 0
    clean_df = pd.DataFrame()

    if not df_valid.empty:
        # Assign parsed & typed columns
        df_valid['order_datetime'] = parsed_order_dts[is_valid_series]
        df_valid['updated_at'] = parsed_updated_dts[is_valid_series]
        df_valid['quantity'] = parsed_quantities[is_valid_series].astype(int)
        df_valid['unit_price'] = parsed_prices[is_valid_series].astype(float)
        df_valid['discount_pct'] = parsed_discounts[is_valid_series].astype(float)
        df_valid['customer_id'] = df_valid['customer_id'].astype(str).str.strip()
        df_valid['product_id'] = df_valid['product_id'].astype(str).str.strip()

        # Normalization
        df_valid['payment_method'] = df_valid['payment_method'].apply(normalize_payment_method)
        df_valid['sales_channel'] = df_valid['sales_channel'].apply(normalize_sales_channel)

        # Calculations
        df_valid['gross_amount'] = (df_valid['quantity'] * df_valid['unit_price']).round(2)
        df_valid['net_amount'] = (df_valid['gross_amount'] * (1.0 - df_valid['discount_pct'] / 100.0)).round(2)

        # Date Key (YYYYMMDD)
        df_valid['date_key'] = df_valid['order_datetime'].dt.strftime("%Y%m%d").astype(int)

        # Deduplication on order_id (keep latest updated_at)
        initial_valid_len = len(df_valid)
        df_valid = df_valid.sort_values(by=['order_id', 'updated_at'], ascending=[True, True])
        clean_df = df_valid.drop_duplicates(subset=['order_id'], keep='last').copy()
        num_duplicates_removed = initial_valid_len - len(clean_df)

    return clean_df, df_quarantine, num_duplicates_removed


# ---------------------------------------------------------------------------
# Task 3 & 4: Star Schema Loading, Idempotency & Incremental Loading
# ---------------------------------------------------------------------------

def load_fact_sales(clean_df: pd.DataFrame, db_path: str) -> Tuple[int, float]:
    """
    Loads clean records into fact_sales with Idempotent & Incremental logic:
    - If order_id does not exist -> INSERT
    - If order_id exists and updated_at is newer -> UPDATE
    - If order_id exists and updated_at is older or equal -> SKIP (Idempotency)

    Returns:
        (rows_loaded_or_updated, net_sales_amount_loaded)
    """
    if clean_df.empty:
        return 0, 0.0

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    cursor = conn.cursor()

    # Get dimension lookup maps
    cursor.execute("SELECT customer_id, customer_key FROM dim_customer;")
    cust_map = dict(cursor.fetchall())

    cursor.execute("SELECT product_id, product_key FROM dim_product;")
    prod_map = dict(cursor.fetchall())

    # Get existing order_ids and updated_at timestamps from fact_sales
    cursor.execute("SELECT order_id, updated_at FROM fact_sales;")
    existing_facts = dict(cursor.fetchall())

    # Ensure dates exist in dim_date
    ensure_date_dimension(clean_df['order_datetime'].tolist(), db_path)

    rows_loaded = 0
    net_sales_loaded = 0.0

    for _, row in clean_df.iterrows():
        order_id = str(row['order_id']).strip()
        customer_key = cust_map[row['customer_id']]
        product_key = prod_map[row['product_id']]
        date_key = int(row['date_key'])
        quantity = int(row['quantity'])
        unit_price = float(row['unit_price'])
        discount_pct = float(row['discount_pct'])
        gross_amount = float(row['gross_amount'])
        net_amount = float(row['net_amount'])
        payment_method = str(row['payment_method'])
        sales_channel = str(row['sales_channel'])
        updated_at = row['updated_at'].strftime("%Y-%m-%d %H:%M:%S")

        if order_id not in existing_facts:
            # New order -> INSERT
            cursor.execute("""
                INSERT INTO fact_sales (
                    order_id, date_key, customer_key, product_key,
                    quantity, unit_price, discount_pct,
                    gross_amount, net_amount, payment_method,
                    sales_channel, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, (
                order_id, date_key, customer_key, product_key,
                quantity, unit_price, discount_pct,
                gross_amount, net_amount, payment_method,
                sales_channel, updated_at
            ))
            rows_loaded += 1
            net_sales_loaded += net_amount
        else:
            # Existing order -> Check incremental watermark / timestamp
            existing_updated_at = existing_facts[order_id]
            if updated_at > existing_updated_at:
                # Newer record -> UPDATE (Upsert)
                cursor.execute("""
                    UPDATE fact_sales SET
                        date_key = ?,
                        customer_key = ?,
                        product_key = ?,
                        quantity = ?,
                        unit_price = ?,
                        discount_pct = ?,
                        gross_amount = ?,
                        net_amount = ?,
                        payment_method = ?,
                        sales_channel = ?,
                        updated_at = ?
                    WHERE order_id = ?;
                """, (
                    date_key, customer_key, product_key,
                    quantity, unit_price, discount_pct,
                    gross_amount, net_amount, payment_method,
                    sales_channel, updated_at, order_id
                ))
                rows_loaded += 1
                net_sales_loaded += net_amount
            else:
                # Same or older updated_at -> IDEMPOTENT SKIP
                pass

    conn.commit()
    conn.close()

    return rows_loaded, round(net_sales_loaded, 2)


def save_quarantine(quarantine_df: pd.DataFrame, config: PipelineConfig):
    """Appends rejected records to SQLite quarantine_records table and quarantine.csv."""
    if quarantine_df.empty:
        return

    # 1. Save to SQLite
    conn = sqlite3.connect(config.db_path)
    cursor = conn.cursor()

    quarantine_records = [
        (
            str(row.get('source_batch', '')),
            str(row.get('order_id', '')),
            str(row.get('order_datetime', '')),
            str(row.get('customer_id', '')),
            str(row.get('product_id', '')),
            str(row.get('quantity', '')),
            str(row.get('unit_price', '')),
            str(row.get('discount_pct', '')),
            str(row.get('payment_method', '')),
            str(row.get('sales_channel', '')),
            str(row.get('updated_at', '')),
            str(row.get('reason_code', '')),
            str(row.get('quarantined_at', datetime.now().isoformat()))
        )
        for _, row in quarantine_df.iterrows()
    ]

    cursor.executemany("""
        INSERT INTO quarantine_records (
            source_batch, order_id, order_datetime, customer_id, product_id,
            quantity, unit_price, discount_pct, payment_method, sales_channel,
            updated_at, reason_code, quarantined_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, quarantine_records)
    conn.commit()
    conn.close()

    # 2. Append to CSV
    file_exists = os.path.exists(config.quarantine_path)
    quarantine_df.to_csv(
        config.quarantine_path,
        mode='a' if file_exists else 'w',
        header=not file_exists,
        index=False,
        encoding='utf-8-sig'
    )


def log_pipeline_run(
    batch_name: str,
    started_at: datetime,
    ended_at: datetime,
    rows_read: int,
    rows_valid: int,
    rows_rejected: int,
    rows_duplicated: int,
    rows_loaded: int,
    net_sales_loaded: float,
    status: str,
    error_message: str,
    config: PipelineConfig
):
    """Records pipeline run metrics to SQLite table and pipeline_run_log.csv."""
    conn = sqlite3.connect(config.db_path)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO pipeline_run_log (
            batch_name, started_at, ended_at, rows_read, rows_valid,
            rows_rejected, rows_duplicated, rows_loaded, net_sales_loaded,
            status, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, (
        batch_name,
        started_at.isoformat(),
        ended_at.isoformat(),
        rows_read,
        rows_valid,
        rows_rejected,
        rows_duplicated,
        rows_loaded,
        net_sales_loaded,
        status,
        error_message
    ))
    conn.commit()
    conn.close()

    # Export / update log CSV
    log_entry = {
        'batch_name': batch_name,
        'started_at': started_at.isoformat(),
        'ended_at': ended_at.isoformat(),
        'rows_read': rows_read,
        'rows_valid': rows_valid,
        'rows_rejected': rows_rejected,
        'rows_duplicated': rows_duplicated,
        'rows_loaded': rows_loaded,
        'net_sales_loaded': net_sales_loaded,
        'status': status,
        'error_message': error_message
    }
    df_log = pd.DataFrame([log_entry])
    file_exists = os.path.exists(config.log_path)
    df_log.to_csv(
        config.log_path,
        mode='a' if file_exists else 'w',
        header=not file_exists,
        index=False,
        encoding='utf-8-sig'
    )


# ---------------------------------------------------------------------------
# Task 5: Master Orchestration Pipeline
# ---------------------------------------------------------------------------

def process_batch(batch_name: str, config: PipelineConfig) -> Dict:
    """Processes a single batch through Extract -> Transform -> Validate -> Load."""
    started_at = datetime.now()
    logger.info(f"--- Starting Processing: {batch_name} ---")

    try:
        # 1. Extract
        df_raw = pd.read_excel(config.dataset_path, sheet_name=batch_name)
        rows_read = len(df_raw)
        logger.info(f"[{batch_name}] Extracted {rows_read} raw records")

        # Get valid Dimension IDs for referential integrity
        conn = sqlite3.connect(config.db_path)
        valid_cust_ids = set(pd.read_sql("SELECT customer_id FROM dim_customer;", conn)['customer_id'])
        valid_prod_ids = set(pd.read_sql("SELECT product_id FROM dim_product;", conn)['product_id'])
        conn.close()

        # 2. Transform & Validate
        clean_df, quarantine_df, rows_duplicated = transform_and_validate_batch(
            df=df_raw,
            source_batch=batch_name,
            valid_customer_ids=valid_cust_ids,
            valid_product_ids=valid_prod_ids
        )

        rows_rejected = len(quarantine_df)
        rows_valid = rows_read - rows_rejected

        logger.info(f"[{batch_name}] Validated: {rows_valid} valid, {rows_rejected} rejected (Quarantine)")

        # 3. Quarantine Storage
        if rows_rejected > 0:
            save_quarantine(quarantine_df, config)
            logger.info(f"[{batch_name}] Saved {rows_rejected} records to quarantine")

        # 4. Load (Idempotent / Incremental)
        rows_loaded, net_sales_loaded = load_fact_sales(clean_df, config.db_path)
        logger.info(f"[{batch_name}] Loaded into Fact: {rows_loaded} rows (Net Sales: {net_sales_loaded:,.2f} THB, Deduplicated: {rows_duplicated})")

        ended_at = datetime.now()
        status = "SUCCESS"
        error_message = ""

        # 5. Log Run
        log_pipeline_run(
            batch_name=batch_name,
            started_at=started_at,
            ended_at=ended_at,
            rows_read=rows_read,
            rows_valid=rows_valid,
            rows_rejected=rows_rejected,
            rows_duplicated=rows_duplicated,
            rows_loaded=rows_loaded,
            net_sales_loaded=net_sales_loaded,
            status=status,
            error_message=error_message,
            config=config
        )

        return {
            'batch_name': batch_name,
            'rows_read': rows_read,
            'rows_valid': rows_valid,
            'rows_rejected': rows_rejected,
            'rows_duplicated': rows_duplicated,
            'rows_loaded': rows_loaded,
            'net_sales_loaded': net_sales_loaded,
            'status': status
        }

    except Exception as e:
        ended_at = datetime.now()
        logger.error(f"[{batch_name}] Pipeline execution failed: {e}", exc_info=True)
        log_pipeline_run(
            batch_name=batch_name,
            started_at=started_at,
            ended_at=ended_at,
            rows_read=0,
            rows_valid=0,
            rows_rejected=0,
            rows_duplicated=0,
            rows_loaded=0,
            net_sales_loaded=0.0,
            status="FAILED",
            error_message=str(e),
            config=config
        )
        if config.error_mode == "fail":
            raise
        return {'batch_name': batch_name, 'status': 'FAILED', 'error': str(e)}


def run_pipeline(config: Optional[PipelineConfig] = None):
    """
    Master pipeline runner:
    1. Initialize schema
    2. Load dimensions
    3. Run batches sequentially (including rerun test)
    4. Print summary KPI report
    """
    if config is None:
        config = PipelineConfig()

    logger.info("=================================================================")
    logger.info("Starting Omnichannel Retail Data Pipeline")
    logger.info(f"Dataset: {config.dataset_path}")
    logger.info(f"Database: {config.db_path}")
    logger.info("=================================================================")

    # Clear old run logs / quarantine files if starting clean
    if os.path.exists(config.quarantine_path):
        os.remove(config.quarantine_path)
    if os.path.exists(config.log_path):
        os.remove(config.log_path)
    if os.path.exists(config.db_path):
        os.remove(config.db_path)

    # 1. Init Database & Dimensions
    init_database(config.db_path)
    load_dimensions(config.dataset_path, config.db_path)

    # 2. Process Batches in Order: batch_1 -> batch_1 (rerun) -> batch_2 -> batch_3
    run_sequence = [
        ("orders_batch_1", "Run 1: Batch 1 Initial Load"),
        ("orders_batch_1", "Run 2: Batch 1 Re-run (Idempotency Test)"),
        ("orders_batch_2", "Run 3: Batch 2 Incremental Load"),
        ("orders_batch_3", "Run 4: Batch 3 Incremental Load")
    ]

    results = []
    for batch_sheet, run_desc in run_sequence:
        logger.info(f"\n>>> {run_desc} <<<")
        res = process_batch(batch_sheet, config)
        res['run_description'] = run_desc
        results.append(res)

    # 3. Overall KPI Summary
    conn = sqlite3.connect(config.db_path)
    total_cust = pd.read_sql("SELECT COUNT(*) AS c FROM dim_customer;", conn)['c'][0]
    total_prod = pd.read_sql("SELECT COUNT(*) AS c FROM dim_product;", conn)['c'][0]
    total_dates = pd.read_sql("SELECT COUNT(*) AS c FROM dim_date;", conn)['c'][0]
    total_facts = pd.read_sql("SELECT COUNT(*) AS c FROM fact_sales;", conn)['c'][0]
    total_net_sales = pd.read_sql("SELECT SUM(net_amount) AS s FROM fact_sales;", conn)['s'][0] or 0.0
    total_quarantine = pd.read_sql("SELECT COUNT(*) AS c FROM quarantine_records;", conn)['c'][0]
    conn.close()

    logger.info("\n" + "=" * 65)
    logger.info("DATA WAREHOUSE KPI & PIPELINE SUMMARY REPORT")
    logger.info("=" * 65)
    print("\n--- Pipeline Run History Table ---")
    df_summary = pd.DataFrame(results)[[
        'run_description', 'rows_read', 'rows_valid', 'rows_rejected',
        'rows_duplicated', 'rows_loaded', 'net_sales_loaded', 'status'
    ]]
    print(df_summary.to_string(index=False))

    print("\n--- Final Data Warehouse Metrics ---")
    print(f"Total Customers in Dimension:  {total_cust:,}")
    print(f"Total Products in Dimension:   {total_prod:,}")
    print(f"Total Dates in Dimension:      {total_dates:,}")
    print(f"Total Orders in Fact Table:    {total_facts:,}")
    print(f"Total Net Sales Revenue:       {total_net_sales:,.2f} THB")
    print(f"Total Quarantined Records:     {total_quarantine:,}")
    print("=" * 65)

    return results


if __name__ == "__main__":
    run_pipeline()
