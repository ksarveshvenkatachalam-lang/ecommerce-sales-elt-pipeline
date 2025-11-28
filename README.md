# E-commerce Sales ELT Pipeline

An end-to-end data pipeline demonstrating the **ELT (Extract-Load-Transform)** methodology using Snowflake, dbt, and Python.

## What is ELT?

**ELT** stands for **Extract, Load, Transform** - a modern data integration approach where:

1. **Extract**: Data is extracted from source systems (APIs, databases, files)
2. **Load**: Raw data is loaded directly into the data warehouse
3. **Transform**: Transformations happen INSIDE the data warehouse using SQL

### ELT vs ETL

| Aspect | ETL | ELT |
|--------|-----|-----|
| Transform Location | Outside warehouse | Inside warehouse |
| Processing | Before loading | After loading |
| Scalability | Limited by ETL server | Leverages warehouse compute |
| Raw Data | Not preserved | Preserved in raw layer |
| Best For | Legacy systems | Cloud data warehouses |

## Project Architecture

```
Source Systems          Snowflake Data Warehouse
     |                         |
     v                         v
[EXTRACT]              [RAW LAYER]
  Python                 Raw tables
  APIs                      |
  Files                     v
     |                 [STAGING]
     v                   dbt models
  [LOAD]                    |
  Snowflake                 v
  COPY INTO           [INTERMEDIATE]
                        Business logic
                            |
                            v
                      [ANALYTICS]
                        Marts/Reports
```

## Project Structure

```
ecommerce-sales-elt-pipeline/
├── extract/                    # EXTRACT phase
│   ├── api_extractor.py       # API data extraction
│   └── file_extractor.py      # File-based extraction
├── load/                       # LOAD phase
│   ├── snowflake_loader.py    # Bulk loading scripts
│   └── sql/
│       ├── create_raw_tables.sql
│       └── copy_commands.sql
├── transform/                  # TRANSFORM phase (dbt)
│   └── dbt/
│       ├── models/
│       │   ├── staging/
│       │   ├── intermediate/
│       │   └── marts/
│       └── dbt_project.yml
├── orchestration/             # Pipeline orchestration
│   └── airflow_dag.py
└── requirements.txt
```

## Data Model

### Source Data: E-commerce Sales
- Orders
- Customers
- Products
- Order Items

### Output Analytics
- Daily Sales Summary
- Customer Lifetime Value
- Product Performance
- Revenue Analytics

## Technologies

- **Snowflake** - Cloud data warehouse
- **dbt** - SQL-based transformations
- **Python** - Extraction and loading scripts
- **SQL** - Data transformations

## Getting Started

1. Clone this repository
2. Set up Snowflake account
3. Configure dbt profile
4. Run extraction scripts
5. Execute dbt models

## License

MIT License
