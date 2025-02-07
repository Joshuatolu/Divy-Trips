# Docker and Airflow ETL Pipeline for ClickHouse to PostgreSQL Integration
This project uses Docker and an Airflow ETL pipeline for extracting trip data from ClickHouse, transforming it, and loading it into a PostgreSQL database. The pipeline is orchestrated using Apache Airflow and includes a stored procedure in PostgreSQL for aggregating data.

## Overview
The project consists of the following components:

1. Dockerfile:

    + Builds a custom Airflow image with additional Python dependencies.
    + Extends the official apache/airflow:2.10.3 image.
    + Installs libraries listed in requirements.txt.

2. Airflow DAG:

    + Defines an ETL pipeline with tasks for:
    + Extracting data from ClickHouse.
    + Loading data into a PostgreSQL staging table (STG.daily_trips).
    + Running a stored procedure (PRC_AGG_TRIGGERS) to aggregate data into EDW tables.

3. PostgreSQL Stored Procedure:

    + Aggregates trip data into two tables:
      + EDW.daily_trip_agg: Daily trip summaries.
      + EDW.vendor_daily_trip_agg: Daily trip summaries by vendor and payment type.

4. Helper Functions:

    + Functions for connecting to ClickHouse and PostgreSQL.
    + Functions for extracting, loading, and transforming data.

### Technologies Used
  - Apache Airflow: Orchestrates the ETL pipeline.
  - ClickHouse: Source database for trip data.
  - PostgreSQL: Target database for storing and aggregating trip data.
  - Docker: Containerizes the Airflow environment.
  - Python: Scripting language for ETL logic.
  - SQLAlchemy: Database connection and ORM.
  - Pandas: Data manipulation and transformation.

>#### Repository Structure
```

├── Dockerfile                  # Custom Airflow image with dependencies
├── dags/                       # Airflow DAGs and ETL scripts
│   ├── main.py                 # Main ETL script
│   ├── Modules/                # Helper modules
│   │   ├── extract.py          # ClickHouse data extraction
│   │   ├── helper.py           # Database connection utilities
│   │   ├── load.py             # PostgreSQL data loading
│   └── raw_files/              # Temporary storage for extracted data
├── requirements.txt            # Python dependencies
└── docker-compose.yaml         # Airflow stack configuration
```
- **Dockerfile:** The Dockerfile extends the official Airflow image and installs additional Python libraries from the 'requirements.txt' file

- **Airflow DAG:** The DAG (Clickhouse_ETL) is scheduled to run daily and includes the following tasks:

    + Pipeline_start: Dummy task to mark the start of the pipeline.
    + extract: Extracts trip data from ClickHouse.
    + stg_load: Loads extracted data into the STG.daily_trips table in PostgreSQL.
    + stg_move: Runs the stored procedure to aggregate data into EDW tables.
    + End_Pipeline: Dummy task to mark the end of the pipeline.

- **PostgreSQL Stored Procedure**
  The stored procedure PRC_AGG_TRIGGERS performs the following operations:

    + Aggregates daily trip data into EDW.daily_trip_agg.
    + Aggregates daily trip data by vendor and payment type into EDW.vendor_daily_trip_agg.
    + Logs the status of the procedure execution in STG.daily_trip_logging.

>### Prerequisites
+ Docker and Docker Compose installed.
+ ClickHouse and PostgreSQL databases accessible.
+ Environment variables configured in .env for database credentials.

**For any questions or issues, please open an issue in the repository. Contributions are welcome! Thank You!!!**
