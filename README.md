
# Financial Market Data Engineering Pipeline (Week 1 Project)

![architecture](https://github.com/user-attachments/assets/fc607220-10a3-47cd-890a-d4c21debd91e)

## 🌐 Project Overview
This project implements an automated data engineering pipeline designed to extract, transform, and load (ETL) financial market data from Yahoo Finance using Python, Apache Spark, and PostgreSQL. The pipeline is orchestrated using Apache Airflow and is scheduled to run multiple times daily to ensure up-to-date insights into the stock performance of major companies.

The primary focus is on developing an end-to-end data pipeline that:
- Automates financial data extraction
- Applies cleaning and time-series feature enrichment
- Efficiently loads the data into a PostgreSQL database for further analysis

This pipeline supports both full historical data initialization and daily incremental updates.

---

## 📊 Business Goal
The goal is to create a reliable and automated pipeline to keep track of selected stock tickers, enabling downstream users or systems to query a live, structured PostgreSQL database for financial insights.

---

## 📃 Key Technologies
- **Python**: Core programming language
- **Yahoo Finance API**: Source of stock data (`yfinance`)
- **Apache Spark**: Data transformation and feature engineering
- **PostgreSQL**: Target data warehouse
- **Airflow**: Workflow orchestration
- **Windows Subsystem for Linux**: Containerization

---

## 🛠️ Project Structure
```bash
financial_data_dag/
├── dags/
│   └── dag_script.py     # Airflow DAG
├── extraction.py                          # Extract script
├── transformation.py                      # Transform script
├── loading.py                             # Load script
├── raw_data/
│   ├── historical_data.csv
│   ├── current_data.csv
│   └── last_data_file.txt
├── transformed_data/                      # Contains final Parquet output
├── postgresql-42.7.5.jar                  # PostgreSQL JDBC driver for Spark
└── .env                                   # Stores DB credentials
```

---

## 🧰 ETL Pipeline Logic

### 1. Extraction (`extraction.py`)
- Downloads either full historical or daily stock data using `yfinance`
- Extracted data includes Date, Open, Close, High, Low, Volume, Dividends, and Stock Splits
- Adds a mapping from ticker to company name
- Saves outputs:
  - `historical_data.csv`: Complete dataset
  - `current_data.csv`: Latest day's extract
  - `last_data_file.txt`: Flag used to inform downstream task which file to process
- Raises `AirflowFailException` on any critical failure

### 2. Transformation (`transformation.py`)
- Loads the appropriate CSV file based on the flag
- Cleans column names and casts data types
- Adds derived time features: Year, Month, Day, Quarter, and Weekday
- Handles missing values by type
- Generates a unique `id` column
- Saves output to `transformed_data/` as Parquet
- Deletes `current_data.csv` after processing
- Fully Spark-based and raises Airflow exceptions if anything fails

### 3. Loading (`loading.py`)
- Reads the transformed Parquet file
- If it's the first run, **overwrites** the target table in PostgreSQL
- For subsequent runs:
  - Loads into a staging table
  - Performs a `NOT EXISTS` insert to avoid duplicates
  - Drops the staging table after use
- Uses environment variables from `.env` for DB credentials
- Protected against insert errors and logs row count

---

## ⚙️ DAG Configuration (`finance_data_pipeline_week1.py`)
- Scheduled to run at `6am`, `12pm`, and `6pm` daily
- Uses `PythonOperator` for each ETL component
- Tasks:
  - `extraction` >> `transformation` >> `loading`
- Configured with retries, email alerts, and `catchup=False`

---

## 📖 How to Run the Project
1. **Set up your environment**
    - Create a `.env` file with the following:
      ```env
      dbname=your_database_name
      user=your_db_user
      password=your_db_password
      host=localhost
      port=5432
      ```

2. **Install dependencies**
    ```bash
    pip install yfinance pandas python-dotenv psycopg2-binary
    # Apache Spark installation assumed
    ```

3. **Start Airflow**
    ```bash
    airflow db init
    airflow users create --role Admin --username admin --password admin --firstname fname --lastname lname --email your@email.com
    airflow scheduler
    airflow webserver
    ```

4. **Place scripts in your `dags/` folder**
    ```bash
    dags/
    ├── dag_script.py
    ├── extraction.py
    ├── transformation.py
    └── loading.py
    ```

5. **Trigger manually (for testing)**
    ```bash
    airflow dags trigger finance_data_dag
    ```

---

## 🐞 Error Handling
- All three ETL scripts use `AirflowFailException` to halt execution on critical errors
- Logging is enabled at all steps
- Flags are used to control file processing and prevent data duplication

---


## 📄 Author
**Kingsley Amegah**  
*Data Engineering Intern* 
*10Alytics
`classrep.dbs.cucg@gmail.com`

---
