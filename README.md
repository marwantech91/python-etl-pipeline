# Python ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=flat-square&logo=python)
![Pandas](https://img.shields.io/badge/Pandas-2.0-150458?style=flat-square&logo=pandas)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)

Python ETL pipeline framework with Pandas, data validation, scheduling, and support for multiple data sources.

## Features

- **Extract** - CSV, JSON, SQL, APIs
- **Transform** - Pandas transformations
- **Load** - Database, files, APIs
- **Validation** - Schema validation
- **Scheduling** - Cron-based jobs
- **Logging** - Comprehensive logging
- **Parallel** - Multi-threaded processing

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run pipeline
python -m etl run sales_pipeline

# Run with schedule
python -m etl schedule
```

## Pipeline Definition

```python
from etl import Pipeline, Extract, Transform, Load

pipeline = Pipeline("sales_etl")

@pipeline.extract
def extract_sales():
    return Extract.from_csv("data/sales.csv")

@pipeline.transform
def clean_data(df):
    return (df
        .dropna(subset=["customer_id", "amount"])
        .assign(amount=lambda x: x["amount"].abs())
        .query("amount > 0"))

@pipeline.transform
def enrich_data(df):
    customers = Extract.from_sql("SELECT * FROM customers", conn)
    return df.merge(customers, on="customer_id", how="left")

@pipeline.load
def load_warehouse(df):
    Load.to_sql(df, "sales_enriched", conn, if_exists="replace")

# Run
pipeline.run()
```

## Extractors

```python
from etl.extractors import Extract

# CSV
df = Extract.from_csv("data.csv", encoding="utf-8")

# JSON
df = Extract.from_json("data.json")

# SQL
df = Extract.from_sql("SELECT * FROM users", connection)

# API
df = Extract.from_api(
    url="https://api.example.com/data",
    headers={"Authorization": "Bearer token"},
    params={"limit": 1000}
)

# Excel
df = Extract.from_excel("data.xlsx", sheet_name="Sheet1")

# Parquet
df = Extract.from_parquet("data.parquet")
```

## Transformers

```python
from etl.transformers import Transform

# Clean
df = Transform.clean(df, {
    "name": "string",
    "email": "email",
    "age": "integer",
    "created_at": "datetime"
})

# Filter
df = Transform.filter(df, "age >= 18 and status == 'active'")

# Aggregate
df = Transform.aggregate(df,
    group_by=["region", "product"],
    aggregations={
        "revenue": "sum",
        "orders": "count",
        "avg_order": ("revenue", "mean")
    }
)

# Join
df = Transform.join(df, customers_df, on="customer_id", how="left")

# Pivot
df = Transform.pivot(df,
    index="date",
    columns="product",
    values="revenue"
)
```

## Loaders

```python
from etl.loaders import Load

# SQL
Load.to_sql(df, "table_name", connection,
    if_exists="replace",  # or "append", "fail"
    chunksize=10000
)

# CSV
Load.to_csv(df, "output.csv", index=False)

# JSON
Load.to_json(df, "output.json", orient="records")

# Parquet
Load.to_parquet(df, "output.parquet")

# API
Load.to_api(df,
    url="https://api.example.com/import",
    method="POST",
    batch_size=100
)
```

## Validation

```python
from etl.validators import Schema, Validator

schema = Schema({
    "customer_id": {"type": "integer", "required": True},
    "email": {"type": "email", "required": True},
    "amount": {"type": "float", "min": 0},
    "status": {"type": "string", "choices": ["active", "inactive"]},
    "created_at": {"type": "datetime"}
})

# Validate DataFrame
errors = Validator.validate(df, schema)

if errors:
    for error in errors:
        print(f"Row {error.row}: {error.field} - {error.message}")

# Validate and filter
valid_df, invalid_df = Validator.split(df, schema)
```

## Scheduling

```python
from etl.scheduler import Scheduler

scheduler = Scheduler()

# Daily at 6 AM
@scheduler.job("0 6 * * *")
def daily_sales():
    sales_pipeline.run()

# Every hour
@scheduler.job("0 * * * *")
def hourly_sync():
    sync_pipeline.run()

# Run scheduler
scheduler.start()
```

## Error Handling

```python
from etl import Pipeline
from etl.handlers import on_error, retry

@pipeline.transform
@retry(attempts=3, delay=60)
def api_transform(df):
    # Will retry 3 times with 60s delay
    return enrich_from_api(df)

@pipeline.on_error
def handle_error(error, context):
    send_alert(f"Pipeline failed: {error}")
    save_failed_data(context.data, "failed_data.csv")
```

## Logging

```python
from etl import Pipeline
from etl.logging import setup_logging

setup_logging(
    level="INFO",
    file="logs/etl.log",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

pipeline = Pipeline("sales_etl")

# Automatic logging
# 2024-01-15 06:00:00 - sales_etl - INFO - Starting pipeline
# 2024-01-15 06:00:01 - sales_etl - INFO - Extract: 10000 rows
# 2024-01-15 06:00:05 - sales_etl - INFO - Transform: 9500 rows
# 2024-01-15 06:00:10 - sales_etl - INFO - Load: 9500 rows inserted
# 2024-01-15 06:00:10 - sales_etl - INFO - Pipeline completed in 10.5s
```

## Parallel Processing

```python
from etl import Pipeline
from etl.parallel import parallel_extract, parallel_transform

@pipeline.extract
@parallel_extract(workers=4)
def extract_multiple():
    return [
        ("sales_2023.csv", {}),
        ("sales_2024.csv", {}),
    ]

@pipeline.transform
@parallel_transform(workers=4)
def process_chunk(chunk):
    return expensive_transformation(chunk)
```

## CLI

```bash
# Run pipeline
python -m etl run <pipeline_name>

# Run with config
python -m etl run <pipeline_name> --config config.yaml

# List pipelines
python -m etl list

# Show pipeline info
python -m etl info <pipeline_name>

# Start scheduler
python -m etl schedule

# Validate data
python -m etl validate data.csv --schema schema.yaml
```

## License

MIT
