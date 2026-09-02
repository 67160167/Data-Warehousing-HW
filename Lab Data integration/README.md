# TechTrove E-Commerce Data Integration Pipeline Lab
**End-to-End Data Integration, Cleaning, Star Schema Modeling & Sales Analytics**

---

## 1. บทนำและภาพรวมระบบ (Overview)

โปรเจกต์นี้เป็นการพัฒนาระบบ **Data Integration Pipeline (ETL)** ด้วยภาษา Python และ Pandas สำหรับบริษัท **TechTrove E-Commerce** ซึ่งจัดเก็บข้อมูลแยกกันในหลายระบบ (Siloed Systems) ได้แก่:
1. **ข้อมูลคำสั่งซื้อรายเดือน (Transaction Orders):** ไฟล์ CSV แยกรายเดือน (`orders_2026_01.csv`, `orders_2026_02.csv`) ที่มีปัญหา Schema Drift (ชื่อคอลัมน์ไม่ตรงกัน, รูปแบบวันที่และส่วนลดต่างกัน)
2. **ข้อมูลลูกค้าจากระบบ CRM:** ไฟล์ CSV (`customers_crm.csv`) ที่มีปัญหาข้อมูลซ้ำ, อีเมลตัวพิมพ์ใหญ่/เล็ก, ช่องว่างส่วนเกิน และชื่อจังหวัดหลากหลายภาษา/ตัวย่อ
3. **ข้อมูลสินค้าจากฝ่ายจัดซื้อ:** ไฟล์ Excel (`product_master.xlsx`) สำหรับตรวจสอบราคาอ้างอิงและสถานะสินค้า (Active Flag)
4. **ข้อมูลการชำระเงินจาก Payment Gateway:** ไฟล์ JSON (`payments.json`) บันทึกเหตุการณ์ธุรกรรมแบบ Nested Structure ซึ่งมีทั้งสถานะ `PAID`, `FAILED`, `REFUNDED` รวมถึงข้อมูลซ้ำ

ระบบจะทำการดึงข้อมูล (Extract), สำรวจคุณภาพข้อมูลเบื้องต้น (Data Profiling), ปรับ Schema ให้สอดคล้องกัน (Schema Alignment), รวมข้อมูล (Concat), ทำความสะอาดและปรับค่าให้เป็นมาตรฐาน (Standardization/Cleaning), เชื่อมโยงข้อมูลและตรวจสอบความถูกต้องตามกติกาทางธุรกิจ (Integration & Business Validation), บันทึกร่องรอยการตรวจสอบ (Data Quality Audit Trail), และสร้างตารางแบบจำลอง **Star Schema** (`dim_customer`, `dim_product`, `fact_sales`) พร้อมรายงานสรุปยอดขายสุทธิแยกตามจังหวัดและหมวดสินค้า

---

## 2. โครงสร้างไฟล์ในโฟลเดอร์ (Directory Structure)

```
Lab Data integration/
│
├── Data_Integration_Lab_TechTrove.docx  # โจทย์และข้อกำหนดของ Lab
├── Data_Integration_Pipeline.ipynb     # Jupyter Notebook พร้อมคำอธิบายและผลรัน Step-by-step
├── pipeline.py                         # สคริปต์หลักรัน Pipeline อัตโนมัติตั้งแต่ต้นจนจบ
├── starter.py                          # สคริปต์เริ่มต้นที่ได้รับการพัฒนาครบทุก TODO
├── requirements.txt                    # รายการ Library Dependencies
├── README.md                           # เอกสารอธิบายระบบและคำตอบคำถามวิเคราะห์
│
├── data/                               # โฟลเดอร์ข้อมูลดิบต้นทาง (ห้ามแก้ไขไฟล์โดยตรง)
│   ├── orders_2026_01.csv
│   ├── orders_2026_02.csv
│   ├── customers_crm.csv
│   ├── product_master.xlsx
│   └── payments.json
│
└── output/                             # โฟลเดอร์ไฟล์ผลลัพธ์ทั้ง 6 ไฟล์ + กราฟ Challenge
    ├── dim_customer.csv                # ตารางมิติลูกค้า (160 แถว)
    ├── dim_product.csv                 # ตารางมิติสินค้า (40 แถว)
    ├── fact_sales.csv                  # ตารางข้อเท็จจริงยอดขายที่ผ่านเกณฑ์ (660 แถว)
    ├── data_quality_report.csv         # รายงานบันทึกความผิดปกติของข้อมูล (186 แถว)
    ├── summary_by_province.csv         # สรุปยอดขายตามจังหวัด (6 จังหวัด)
    ├── summary_by_category.csv         # สรุปยอดขายตามหมวดสินค้า (4 หมวด)
    └── data_quality_funnel.png         # แผนภาพ Data Quality Funnel (Challenge)
```

---

## 3. วิธีการติดตั้งและรันระบบ (How to Run)

### ความต้องการของระบบ (Prerequisites)
- Python 3.10 ขึ้นไป
- Dependencies: `pandas>=2.0`, `openpyxl>=3.1`, `matplotlib>=3.7`

### การติดตั้ง Libraries
```bash
pip install -r requirements.txt
pip install matplotlib
```

### วิธีที่ 1: รันผ่าน Command Line (CLI)
รันสคริปต์ `pipeline.py` เพื่อประมวลผลกระบวนการ ETL ทั้งหมด ตรวจสอบ Assertions และสร้างไฟล์ผลลัพธ์ใน `output/`:
```bash
python pipeline.py
```

### วิธีที่ 2: รันผ่าน Jupyter Notebook
เปิดและรันไฟล์ `Data_Integration_Pipeline.ipynb`:
```bash
jupyter notebook Data_Integration_Pipeline.ipynb
```

---

## 4. โครงสร้าง Star Schema & Data Lineage

### การกำหนด Grain
- **Fact Table Grain:** *“หนึ่งรายการคำสั่งซื้อที่ผ่านการตรวจสอบคุณภาพข้อมูล ชำระเงินสำเร็จ (PAID) ต่อหนึ่ง order_id”*

### แผนภาพ Star Schema (ER Diagram)

```
        +----------------------------------------+
        |              dim_customer              |
        +----------------------------------------+
        | * customer_id  (PK)                    |
        |   full_name                            |
        |   email                                |
        |   province                             |
        |   signup_date                          |
        +-------------------+--------------------+
                            |
                            | 1:N
                            v
+--------------------------------------------------------+          +------------------------------------+
|                       fact_sales                       |          |            dim_product             |
+--------------------------------------------------------+          +------------------------------------+
| * order_id       (PK)                                  |          | * product_id     (PK)              |
|   order_date                                           |          |   product_name                     |
|   customer_id    (FK -> dim_customer.customer_id)      |  N:1     |   category                         |
|   product_id     (FK -> dim_product.product_id)        |--------->|   standard_price                   |
|   quantity                                             |          |   active_flag                      |
|   unit_price                                           |          +------------------------------------+
|   discount                                             |
|   channel                                              |
|   payment_id                                           |
|   payment_method                                       |
|   net_sales      [qty * unit_price * (1 - discount)]   |
+--------------------------------------------------------+
```

---

## 5. การเปรียบเทียบคุณภาพข้อมูลก่อนและหลังทำ Data Integration

```
[ข้อมูลดิบคำสั่งซื้อ (ม.ค. + ก.พ.)]       752 แถว (100.0%)
               │
               ▼
[หลังลบข้อมูลซ้ำ (keep='last')]          750 แถว (99.7%)
               │
               ▼
[ผ่านเกณฑ์ Master Data & Qty/Price]    724 แถว (96.3%)
               │
               ▼
[ชำระเงินสำเร็จ PAID (fact_sales)]     660 แถว (87.8%)  ==>  ยอดขายสุทธิ 10,224,044.08 บาท
```

| ชุดข้อมูล / ตัวแปร | ข้อมูลดิบก่อน Integration (Before) | ข้อมูลหลัง Integration (After) | การจัดการและเหตุผลทางธุรกิจ |
| :--- | :--- | :--- | :--- |
| **Orders Jan (`orders_2026_01.csv`)** | 361 แถว, วันที่ ISO, ส่วนลดทศนิยม | รวมเข้าสู่แกนมาตรฐานเดียวกัน | ปรับ Format ให้สอดคล้องกัน |
| **Orders Feb (`orders_2026_02.csv`)** | 391 แถว, คอลัมน์ `ordered_at`, `qty`, `discount_pct` (`5%`, `10%`) | ปรับ Schema ตรงกับเดือน ม.ค. | Rename คอลัมน์, แปลงเปอร์เซ็นต์เป็นตัวเลข float [0, 1], แปลงวันที่เป็น ISO |
| **Combined Orders** | 752 แถว | **750 แถว** (Deduplicated) | ลบรายการซ้ำ 2 แถว (`ORD000056`, `ORD000416`) โดยเก็บข้อมูลล่าสุด (`keep='last'`) |
| **Customer Master (`customers_crm.csv`)** | 163 แถว, 3 Duplicate ID, 5 ค่าว่างใน Email, 6 รูปแบบจังหวัดไม่มาตรฐาน | **160 แถว** (`dim_customer.csv`) | Deduplicate ลูกค้า (`C0012`, `C0045`, `C0088`), ตัดช่องว่าง, แปลง Email เป็นตัวพิมพ์เล็ก, แปลงจังหวัดเป็นชื่อภาษาไทยมาตรฐาน |
| **Product Master (`product_master.xlsx`)** | 40 แถว | **40 แถว** (`dim_product.csv`) | ตัดช่องว่างข้อความ ตรวจสอบความถูกต้อง |
| **Payment Events (`payments.json`)** | 752 Nested Events, 1 Duplicate Order, 47 FAILED, 18 REFUNDED | **751 Unique Events** | แตก Nested JSON, Deduplicate `order_id` (`ORD000101`) |
| **Fact Sales Table** | 750 แถวคำสั่งซื้อ | **660 ธุรกรรมที่ผ่านเกณฑ์** (`fact_sales.csv`) | คัดกรองตามกติกาธุรกิจ: `quantity > 0`, `unit_price > 0`, `discount` ในช่วง 0–1, รหัสพบใน Master, สถานะเป็น `PAID` (ตัดออก 90 รายการ พร้อมบันทึกลง DQ Report) |

---

## 6. คำตอบคำถามวิเคราะห์เชิงธุรกิจ 6 ข้อ (Answers to Analysis Questions)

### **ข้อ 1: หลังรวมไฟล์ orders มีจำนวนแถวเท่าใด และเหลือกี่แถวหลังลบ duplicate?**
- **จำนวนแถวหลังรวมไฟล์:** รวมได้ทั้งหมด **752 แถว** (เดือน ม.ค. 361 แถว + เดือน ก.พ. 391 แถว รวมด้วย `pd.concat(..., ignore_index=True)`)
- **จำนวนแถวที่เหลือหลังลบ duplicate:** เหลือ **750 แถว** (ลบแถวซ้ำออกไป 2 แถว ได้แก่ `ORD000056` และ `ORD000416` โดยเลือกเก็บข้อมูลล่าสุด `keep='last'` ตามกติกาข้อ 1)

---

### **ข้อ 2: มีแถวที่ customer_id หรือ product_id ไม่พบใน Master Data อย่างละกี่แถว?**
- **customer_id ไม่พบใน Master Data (`customers_crm.csv`):** มีจำนวน **22 แถว** (ได้แก่รหัสที่ไม่มีอยู่ใน CRM: `C0161`, `C0162`, `C0163`, `C0164`, `C0165`)
- **product_id ไม่พบใน Master Data (`product_master.xlsx`):** มีจำนวน **2 แถว** (ได้แก่รหัสสินค้า `P999` ซึ่งไม่มีในฐานข้อมูลฝ่ายจัดซื้อ)

---

### **ข้อ 3: มียอดขายที่ใช้ได้จริงกี่ธุรกรรม และยอดขายสุทธิรวมเท่าใด?**
- **จำนวนธุรกรรมยอดขายที่ใช้ได้จริง (Fact Sales):** **660 ธุรกรรม** (จาก 750 แถว ถูกตัดออก 90 แถว เนื่องจากผิดเงื่อนไข Quantity $\le 0$, Unit Price สูญหาย, รหัสไม่พบใน Master, หรือสถานะชำระเงินไม่ใช่ `PAID`)
- **ยอดขายสุทธิรวม (Total Net Sales):** **10,224,044.08 บาท**  
  *(คำนวณจากสูตร: $\text{net\_sales} = \text{quantity} \times \text{unit\_price} \times (1 - \text{discount})$)*

---

### **ข้อ 4: จังหวัดใดมียอดขายสุทธิสูงสุด?**
- **กรุงเทพมหานคร** มียอดขายสุทธิสูงสุด อยู่ที่ **2,612,955.87 บาท** (จำนวน 154 คำสั่งซื้อ ปริมาณสินค้ารวม 323 ชิ้น)

#### ตารางสรุปยอดขายแยกตามจังหวัด (`summary_by_province.csv`):
| อันดับ | จังหวัด (Province) | จำนวนคำสั่งซื้อ (Orders) | ปริมาณสินค้า (Quantity) | ยอดขายสุทธิ (Net Sales THB) |
| :---: | :--- | :---: | :---: | :---: |
| 1 | **กรุงเทพมหานคร** | 154 | 323 | **2,612,955.87** |
| 2 | **ขอนแก่น** | 110 | 225 | **2,031,942.50** |
| 3 | **ระยอง** | 120 | 248 | **1,523,169.34** |
| 4 | **เชียงใหม่** | 104 | 206 | **1,477,338.45** |
| 5 | **ภูเก็ต** | 86 | 164 | **1,427,389.28** |
| 6 | **ชลบุรี** | 86 | 171 | **1,151,248.64** |
| **รวม** | **ทั้งหมด** | **660** | **1,337** | **10,224,044.08** |

---

### **ข้อ 5: หมวดสินค้าใดมียอดขายสุทธิสูงสุด?**
- หมวดสินค้า **Smartphone** มียอดขายสุทธิสูงสุด อยู่ที่ **3,092,117.34 บาท** (จำนวน 178 คำสั่งซื้อ ปริมาณสินค้ารวม 384 ชิ้น)

#### ตารางสรุปยอดขายแยกตามหมวดสินค้า (`summary_by_category.csv`):
| อันดับ | หมวดสินค้า (Category) | จำนวนคำสั่งซื้อ (Orders) | ปริมาณสินค้า (Quantity) | ยอดขายสุทธิ (Net Sales THB) |
| :---: | :--- | :---: | :---: | :---: |
| 1 | **Smartphone** | 178 | 384 | **3,092,117.34** |
| 2 | **Accessory** | 180 | 338 | **2,710,583.47** |
| 3 | **Notebook** | 161 | 324 | **2,221,495.27** |
| 4 | **Smart Home** | 141 | 291 | **2,199,848.00** |
| **รวม** | **ทั้งหมด** | **660** | **1,337** | **10,224,044.08** |

---

### **ข้อ 6: หากสลับลำดับ merge ก่อน cleaning ผลลัพธ์หรือความเชื่อมั่นของข้อมูลเปลี่ยนอย่างไร?**
หากทำการ Merge ข้อมูลดิบก่อน Clean & Standardize จะส่งผลกระทบอย่างรุนแรงต่อความถูกต้องและความเชื่อมั่นของข้อมูล 4 ด้าน:

1. **เกิดปัญหา Cardinality Explosion (ข้อมูลเบิ้ลซ้ำจาก Cartesian Product):**  
   ในชุดข้อมูลดิบ Customer มีรหัสลูกค้าซ้ำ 3 ราย (`C0012`, `C0045`, `C0088`) และ Payments มีรหัสคำสั่งซื้อซ้ำ (`ORD000101`) หาก Merge ก่อน Deduplicate ความสัมพันธ์จะกลายเป็น **Many-to-Many Fan-out** ทำให้แถวคำสั่งซื้อถูกคูณเบิ้ล ส่งผลให้ยอดขายรวม (Net Sales) และจำนวนนับออเดอร์ **บวมเกินจริง (Double Counting)**
2. **เกิดปัญหา Unmatched Join Loss จากความไม่เป็นมาตรฐานของข้อความ:**  
   ข้อมูลลูกค้าดิบมี Whitespace นำหน้า/ตามหลัง และชื่อจังหวัดสะกดหลายภาษา (`Chonburi`, `Bangkok`, `กทม.`, `Chiang Mai`, `ขอนเเก่น`) หาก Merge หรือ Group By ก่อน Standardize ข้อมูลที่มีช่องว่างจะไม่สามารถ Join Foreign Key ได้ และการจัดกลุ่มยอดขายรายจังหวัดจะแตกกระจัดกระจาย
3. **ข้อผิดพลาดทางชนิดข้อมูลและสูตรคำนวณ (Computation & Type Failures):**  
   คอลัมน์ `discount_pct` ในเดือน ก.พ. เก็บเป็นสตริง เช่น `'5%'`, `'10%'` และมี `unit_price` ที่เป็นค่าว่าง (NaN) หรือ `quantity` ติดลบ หาก Merge แล้วนำไปคำนวณ `net_sales` ทันที จะทำให้โปรแกรมเกิด `TypeError` ล้มเหลว หรือได้ผลลัพธ์ยอดขายที่ผิดเพี้ยน
4. **สูญเสียความสามารถในการตรวจสอบย้อนกลับ (Loss of Audit Trail & Lineage):**  
   การ Clean และ Validate ทีละขั้นตอนช่วยให้เราสามารถบันทึกและแยกแยะ Root Cause ใน `data_quality_report.csv` ได้อย่างเป็นระบบและโปร่งใส หาก Merge ก่อน จะไม่สามารถระบุสาเหตุที่แท้จริงของความผิดพลาดในแต่ละแถวได้

---

## 7. Challenge เพิ่มเติม (+2 คะแนน)

### ฟังก์ชัน `validate_data(df_fact, df_cust, df_prod)`
ระบบได้สร้างฟังก์ชัน Assertions ตรวจสอบความถูกต้องสมบูรณ์ของข้อมูล 4 มิติ:
- **Uniqueness Check:** ตรวจสอบ Primary Key ห้ามซ้ำ (`order_id`, `customer_id`, `product_id`) $\rightarrow$ **PASS**
- **Referential Integrity Check:** ตรวจสอบ Foreign Key 100% ต้องเชื่อมโยงได้ ไม่พบ Orphan Record $\rightarrow$ **PASS**
- **Domain/Range Check:** `quantity > 0`, `unit_price > 0`, `0 <= discount <= 1`, `net_sales >= 0` $\rightarrow$ **PASS**
- **Completeness Check:** คอลัมน์สำคัญทั้งหมดต้องไม่มีค่า Null $\rightarrow$ **PASS**

### กราฟ Data Quality Funnel
จัดทำแผนภาพแสดงการไหลของข้อมูลตั้งแต่ Raw Orders $\rightarrow$ Deduplicated Orders $\rightarrow$ Valid Master & Price/Qty $\rightarrow$ Fully Paid Fact Sales และบันทึกเป็นไฟล์ภาพ [`output/data_quality_funnel.png`](output/data_quality_funnel.png)
