# ELT Pipeline

This directory contains the **Extract–Load–Transform (ELT)** implementation for the Big Data Final Project. The pipeline ingests Airbnb datasets, loads raw data into PostgreSQL, and performs all transformations using SQL inside the database.

---

## 📁 Directory Structure

```
elt_pipeline/
│
├── 01_extract_load.py      # Extract datasets and load raw data into PostgreSQL
├── 02_merge_data.sql       # Merge multiple raw datasets into a single table
├── 03_fix_type_column.sql  # Fix and cast column data types
├── 05_cleaning_data.sql    # Data cleaning (missing values, duplicates, outliers)
├── 06_validation.sql       # Data quality validation rules
├── 07_move_data.sql        # Move cleaned data from staging to warehouse
├── config.py               # Database connection configuration
└── README.md               # ELT pipeline documentation
```

---

## 🔄 ELT Workflow Overview

The ELT process in this project follows these steps:

### 1. Extract
- Raw datasets are obtained from Kaggle using Python.
- Data is stored locally and prepared for loading without transformation.

**Script:** `01_extract_load.py`

---

### 2. Load
- Extracted data is loaded **as-is** into PostgreSQL.
- Data is stored in the `raw` schema to preserve original values for auditing and profiling.

**Script:** `01_extract_load.py`

---

### 3. Transform
All transformations are executed **inside PostgreSQL** using SQL scripts.

| Step | Description | File |
|-----|-------------|------|
| Merge | Combine multiple raw sources | `02_merge_data.sql` |
| Type Casting | Convert columns to correct data types | `03_fix_type_column.sql` |
| Cleaning | Handle missing values, duplicates, outliers | `05_cleaning_data.sql` |
| Validation | Check data quality rules | `06_validation.sql` |
| Load to Warehouse | Move final data to warehouse schema | `07_move_data.sql` |

---

## 🧱 Database Layers

| Schema | Purpose |
|------|--------|
| `raw` | Store unprocessed raw data |
| `staging` | Store cleaned and intermediate data |
| `warehouse` | Store final data ready for analysis |

---

## ⚙️ Configuration

Update database credentials in `config.py` before running the pipeline:

```python
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "bigdata_airbnb",
    "user": "postgres",
    "password": "your_password"
}
```

---

## ▶️ Execution Order

1. Run extract and load:
```bash
python 01_extract_load.py
```

2. Run SQL scripts sequentially in PostgreSQL:
```sql
\i 02_merge_data.sql
\i 03_fix_type_column.sql
\i 05_cleaning_data.sql
\i 06_validation.sql
\i 07_move_data.sql
```

---

## 📌 Notes
- No transformation is applied during extract or load stages.
- All data cleaning, validation, and feature preparation are handled in SQL.
- This design follows ELT principles and supports reproducibility and auditability.

---

## 🔗 Repository

Main project repository:
https://github.com/Aizar-yzd/bigdata-final-project

---

*Last updated: 2026*

