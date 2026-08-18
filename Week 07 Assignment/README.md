# Omnichannel Retail Data Pipeline Engineering Lab
**Incremental & Idempotent ETL Pipeline with Star Schema Architecture**

---

## 1. บทนำและภาพรวมระบบ (Overview)
โปรเจกต์นี้เป็นการพัฒนาระบบ **ETL Data Pipeline** ด้วยภาษา Python เพื่อดึงข้อมูลธุรกรรมยอดขายค้าปลีก (Omnichannel Retail Orders) จากไฟล์ Dataset ต้นทาง นำมาผ่านกระบวนการตรวจสอบคุณภาพข้อมูล (Data Quality Validation), การปรับแต่งข้อมูลให้อยู่ในรูปแบบมาตรฐาน (Normalization/Transformation), การจัดการข้อมูลที่มีข้อผิดพลาดแยกเข้าสู่พื้นที่กักกัน (**Quarantine System**), และนำเข้าสู่ฐานข้อมูล **SQLite Data Warehouse** ตามโครงสร้าง **Star Schema** โดยรองรับการทำงานแบบ **Incremental Loading** และมีความเป็น **Idempotent** (รันซ้ำแล้วไม่เกิดข้อมูลซ้ำซ้อน)

---

## 2. วิธีการติดตั้งและสภาพแวดล้อมที่ต้องใช้ (Installation)

### ความต้องการของระบบ (Prerequisites)
- Python 3.10 ขึ้นไป
- Libraries ที่จำเป็น: `pandas`, `openpyxl`, `numpy`

### การติดตั้ง Dependencies
เปิด Terminal หรือ Command Prompt ในโฟลเดอร์โปรเจกต์ แล้วติดตั้ง Library:
```bash
pip install pandas openpyxl numpy
```

---

## 3. วิธีการรันระบบ (How to Run)

### วิธีที่ 1: รันผ่าน Command Line (CLI)
รันสคริปต์หลักเพื่อประมวลผลทั้ง 4 รอบอัตโนมัติ:
```bash
python pipeline.py
```

### วิธีที่ 2: รันผ่าน Jupyter Notebook
เปิดไฟล์ `notebook.ipynb` เพื่อรันและดูผลการวิเคราะห์ข้อมูลแบบ Step-by-step:
```bash
jupyter notebook notebook.ipynb
```

### การตรวจสอบความถูกต้องตามเกณฑ์ Acceptance Tests
รันสคริปต์ตรวจสอบความถูกต้องของฐานข้อมูลและเงื่อนไขทั้งหมด:
```bash
python verify_dw.py
```

---

## 4. โครงสร้าง Star Schema & Grain Definition

### การกำหนด Grain ของ Fact Table
- **Grain:** *“หนึ่งรายการสั่งซื้อสินค้าที่ผ่านการตรวจสอบคุณภาพข้อมูล ต่อหนึ่ง order_id (1 Validated Order Transaction Line per order_id)”*

### แผนภาพความสัมพันธ์ (Star Schema ER Diagram)

```
        +----------------------------------------+
        |              dim_customer              |
        +----------------------------------------+
        | * customer_key (PK, Auto-inc)          |
        |   customer_id  (UK, Cdddd)             |
        |   customer_name                        |
        |   province                             |
        |   segment                              |
        +----------------------------------------+
                            | 1
                            |
                            | N
+-----------------------------------+        +----------------------------------------+
|             dim_date              |        |               fact_sales               |
+-----------------------------------+        +----------------------------------------+
| * date_key  (PK, YYYYMMDD)        |<---1--N| * order_id       (PK, Transaction ID)  |
|   full_date (UK, YYYY-MM-DD)      |        |   date_key       (FK -> dim_date)      |
|   day       (1-31)                |        |   customer_key   (FK -> dim_customer)  |
|   month     (1-12)                |        |   product_key    (FK -> dim_product)   |
|   quarter   (1-4)                 |        |   quantity       (INTEGER > 0)         |
|   year      (e.g. 2026)           |        |   unit_price     (REAL > 0)            |
+-----------------------------------+        |   discount_pct   (REAL 0-100)          |
                            | 1              |   gross_amount   (quantity * unit_price|
                            |                |   net_amount     (gross * (1 - disc%)) |
                            | N              |   payment_method (Normalized)          |
        +------------------------------------+   sales_channel  (Normalized)          |
        |              dim_product           |   updated_at     (ISO Timestamp)       |
        +------------------------------------+----------------------------------------+
        | * product_key (PK, Auto-inc)       |
        |   product_id  (UK, Pddd)           |
        |   product_name                     |
        |   category                         |
        +------------------------------------+
```

---

## 5. กฎคุณภาพข้อมูล (Data Quality & Normalization Rules)

1. **Safe Type Parsing:** ใช้ `errors='coerce'` ในการแปลงวันที่และตัวเลขอย่างปลอดภัย
2. **Category Standardization:**
   - `payment_method`: ปรับตัวพิมพ์ให้เป็นมาตรฐาน (`Credit Card`, `PromptPay`, `Bank Transfer`, `Cash`)
   - `sales_channel`: แปลงค่า `E-Commerce` เป็น `Online` ตาม Data Dictionary
3. **Data Quality Validation Rules:**
   - `INVALID_DATETIME`: วันที่ในคำสั่งซื้อไม่สามารถแปลงเป็นวันที่ที่ถูกต้องได้
   - `UNKNOWN_CUSTOMER`: รหัสลูกค้าเป็นค่าว่าง หรือไม่มีอยู่ในฐานข้อมูลลูกค้า (`dim_customer`)
   - `UNKNOWN_PRODUCT`: รหัสสินค้าเป็นค่าว่าง หรือไม่มีอยู่ในฐานข้อมูลสินค้า (`dim_product`)
   - `INVALID_QUANTITY`: จำนวนสินค้าไม่ใช่จำนวนเต็ม หรือ $\le 0$ หรือ $> 20$
   - `INVALID_UNIT_PRICE`: ราคาต่อหน่วยไม่ใช่ตัวเลข หรือ $\le 0$
   - `INVALID_DISCOUNT_PCT`: ส่วนลดไม่อยู่ในช่วง $0 - 100\%$
4. **Quarantine Handling:** แถวที่ไม่ผ่านเกณฑ์จะถูกแยกบันทึกลงในไฟล์ `quarantine.csv` และตาราง `quarantine_records` ใน SQLite พร้อมระบุ `reason_code` และ `source_batch` โดยไม่ทำให้ Pipeline หยุดทำงาน
5. **Deduplication & Incremental Upsert:** 
   - กำจัดรายการซ้ำด้วย `order_id` โดยเลือกเก็บเรคคอร์ดที่มี `updated_at` ล่าสุด
   - หากพบ `order_id` เดิมที่มี `updated_at` ใหม่กว่า ระบบจะทำการ Update ข้อมูลเดิมใน Fact Table (Incremental Upsert)
   - หากพบ `order_id` เดิมที่มี `updated_at` เก่ากว่าหรือเท่าเดิม ระบบจะไม่นำเข้าซ้ำ (Idempotency)

---

## 6. ผลการรัน Pipeline และหลักฐาน (Run Log & Verification)

### สรุปผลการรันทั้ง 4 รอบ (Pipeline Run History)
สูตรตรวจสอบ: $\text{rows\_read} = \text{rows\_valid} + \text{rows\_rejected}$ ก่อนทำการ Deduplicate

| Run ID | รอบการทำงาน (Batch Description) | Read | Valid | Rejected (Quarantine) | Duplicated | Loaded into Fact | Net Sales Loaded (THB) | Status |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | Run 1: Batch 1 Initial Load | 420 | 386 | 34 | 0 | 386 | 974,108.90 | **SUCCESS** |
| **2** | Run 2: Batch 1 Re-run (Idempotency Test) | 420 | 386 | 34 | 0 | **0** | **0.00** | **SUCCESS** |
| **3** | Run 3: Batch 2 Incremental Load | 424 | 384 | 40 | 1 | 383 | 947,719.44 | **SUCCESS** |
| **4** | Run 4: Batch 3 Incremental Load | 424 | 386 | 38 | 3 | 382 | 893,196.13 | **SUCCESS** |

### ภาพรวมตัวชี้วัดใน Data Warehouse หลังรันเสร็จสมบูรณ์
- **จำนวนลูกค้าใน Dimension (`dim_customer`):** 180 ราย
- **จำนวนสินค้าใน Dimension (`dim_product`):** 48 รายการ
- **จำนวนวันที่ใน Dimension (`dim_date`):** 169 วัน
- **จำนวนรายการขายใน Fact Table (`fact_sales`):** 1,150 รายการ
- **ยอดขายสุทธิรวม (Total Net Sales):** 2,812,461.57 บาท
- **จำนวนแถวที่ถูกส่งไป Quarantine:** 146 รายการ

---

## 7. บทวิเคราะห์และสะท้อนคิด (Reflection)
**หัวข้อ: เหตุใด Availability จึงมักสำคัญกว่า Strictness ใน Production Pipeline**

> ในระบบ Production Data Pipeline ระดับองค์กร การรักษา **Availability (ความพร้อมใช้งานและความต่อเนื่องของข้อมูล)** มีความสำคัญสูงกว่าการใช้ **Strictness (ความเข้มงวดแบบ Fail-All)** เป็นอย่างมาก เพราะในระบบจริงที่มีธุรกรรมไหลเข้ามาอย่างต่อเนื่อง หากมีข้อมูลผิดพลาดเพียงไม่กี่แถวแล้วทำให้ Pipeline ทั้งระบบล่ม (Crash) ธุรกิจจะไม่สามารถอัปเดต Dashboard, ไม่สามารถออกรายงานยอดขายประจำวันได้ และส่งผลกระทบต่อระบบงานปลายทางทันที 
>
> แนวปฏิบัติที่ดีในวิศวกรรมข้อมูลจึงเลือกใช้กลยุทธ์ **Quarantine Pattern** หรือ **Dead Letter Queue (DLQ)** เพื่อแยกเฉพาะแถวที่มีปัญหาออกไปตรวจสอบและแก้ไขในภายหลัง พร้อมระบุ `reason_code` อย่างโปร่งใส ในขณะที่ข้อมูลถูกต้องส่วนใหญ่ (Good Data) จะยังคงสามารถไหลเข้าสู่ Data Warehouse ได้ตามเวลาจริง (SLA) ซึ่งช่วยสร้างสมดุลระหว่างความสมบูรณ์ถูกต้องของข้อมูลและความต่อเนื่องทางธุรกิจได้อย่างมีประสิทธิภาพสูงสุด

---

## 8. โครงสร้างไฟล์ในโฟลเดอร์งาน (Deliverables)
- `pipeline.py`: โค้ด ETL Pipeline ฉบับสมบูรณ์ที่สามารถรันผ่าน CLI ได้ทันที
- `notebook.ipynb`: Jupyter Notebook สำหรับรัน ทดสอบ และวิเคราะห์ข้อมูลใน Data Warehouse
- `retail_dw.db`: SQLite Database ที่เก็บข้อมูล Star Schema, Run Log และ Quarantine
- `quarantine.csv`: ไฟล์บันทึกข้อมูลที่ไม่ผ่านเกณฑ์พร้อมเหตุผลและ batch ต้นทาง
- `pipeline_run_log.csv`: ไฟล์บันทึกประวัติการรันและตัวชี้วัดในแต่ละรอบ
- `verify_dw.py`: สคริปต์ Automated Test ตรวจสอบ Acceptance Criteria
- `README.md`: รายงานสรุปเอกสารฉบับนี้
