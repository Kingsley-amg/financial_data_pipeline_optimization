def data_loading():
    import psycopg2
    import os
    import logging
    from dotenv import load_dotenv
    from pyspark.sql import SparkSession
    from pyspark.sql.utils import AnalysisException
    from airflow.exceptions import AirflowFailException  # Ensures Airflow task fails on error

    # Load database and environment variables from the .env file
    load_dotenv(override=True)

    # Set up logging format
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Load PostgreSQL credentials from environment
    db_name = os.getenv("dbname")
    db_user = os.getenv("user")
    db_password = os.getenv("password")
    db_host = os.getenv("host")
    db_port = os.getenv("port")

    # Function to establish a database connection to PostgreSQL
    def get_db_connection():
        try:
            return psycopg2.connect(
                database=db_name,
                user=db_user,
                password=db_password,
                host=db_host,
                port=db_port,
                options="-c search_path=finance_schema"
            )
        except psycopg2.DatabaseError as e:
            logging.error(f"Database connection failed: {e}")
            raise AirflowFailException("Could not connect to PostgreSQL database.")

    # Function to create finance_data table inside finance_schema if it does not already exist
    def create_table_if_not_exists():
        conn = get_db_connection()
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE SCHEMA IF NOT EXISTS finance_schema;

                    CREATE TABLE IF NOT EXISTS finance_schema.finance_data (
                        "id" BIGINT PRIMARY KEY,
                        "Date" DATE,
                        "Year" INT,
                        "Month" INT,
                        "Day" INT,
                        "Quarter" INT,
                        "Weekday" VARCHAR(20),
                        "Ticker" VARCHAR(10),
                        "Company" VARCHAR(50),
                        "Open" DOUBLE PRECISION,
                        "High" DOUBLE PRECISION,
                        "Low" DOUBLE PRECISION,
                        "Close" DOUBLE PRECISION,
                        "Volume" BIGINT,
                        "Dividends" DOUBLE PRECISION,
                        "stock_splits" DOUBLE PRECISION
                    );
                ''')
                conn.commit()
                logging.info("Database schema and table created successfully.")
            except psycopg2.Error as e:
                logging.error(f"Error creating schema or table: {e}")
                raise AirflowFailException("Error creating schema or table in PostgreSQL.")
            finally:
                cursor.close()
                conn.close()

    # Ensure the target table exists before inserting
    create_table_if_not_exists()

    # Initialize Spark session for reading and writing data
    try:
        spark = SparkSession.builder \
            .appName("FinancialDataLoading") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.jars", "/home/airflow/airflow/financial_data_dag/postgresql-42.7.5.jar") \
            .getOrCreate()
        logging.info("SparkSession initialized successfully.")
    except Exception as e:
        logging.error(f"Spark session initialization failed: {e}")
        raise AirflowFailException("Spark initialization failed.")

    try:
        # Define the directory structure and file paths
        base_dir = "/home/airflow/airflow/financial_data_dag"
        raw_data_dir = os.path.join(base_dir, "raw_data")
        transformed_data_path = os.path.join(base_dir, "transformed_data")
        flag_file_path = os.path.join(raw_data_dir, "last_data_file.txt")

        # Read the flag file that tells whether to load historical or current data
        if not os.path.exists(flag_file_path):
            logging.error("Missing last_data_file.txt to determine run type.")
            raise AirflowFailException("Flag file not found.")

        with open(flag_file_path, "r") as f:
            last_data_file = f.read().strip()

        # Determine run type based on flag content
        run_type = "initial" if last_data_file == "historical_data.csv" else "incremental"
        logging.info(f"Detected run type: {run_type}")

        # Read the transformed parquet file
        df = spark.read.parquet(transformed_data_path)
        if df.rdd.isEmpty():
            logging.warning("Transformed data is empty. No records to load.")
            raise AirflowFailException("No data found in transformed Parquet file.")

        # Repartition the DataFrame to optimize parallel writes to the database
        df = df.repartition(4)
        logging.info("Transformed data loaded and repartitioned.")

        # Define JDBC connection parameters
        jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}?currentSchema=finance_schema"
        jdbc_properties = {
            "user": db_user,
            "password": db_password,
            "driver": "org.postgresql.Driver"
        }

        if run_type == "initial":
            # Overwrite the table with full historical data
            df.write \
                .option("batchsize", "1000") \
                .option("rewriteBatchedStatements", "true") \
                .jdbc(
                    url=jdbc_url,
                    table="finance_data",
                    mode="overwrite",
                    properties=jdbc_properties
                )
            logging.info("Historical data loaded using OVERWRITE.")
        else:
            # Load current data into a staging table first
            staging_table = "finance_data_staging"

            df.write \
                .option("batchsize", "1000") \
                .option("rewriteBatchedStatements", "true") \
                .jdbc(
                    url=jdbc_url,
                    table=staging_table,
                    mode="overwrite",
                    properties=jdbc_properties
                )
            logging.info("Current data written to staging table.")

            # Insert only new records into the main table (avoid duplicates on 'id')
            conn = get_db_connection()
            if conn:
                try:
                    cursor = conn.cursor()
                    cursor.execute(f'''
                        INSERT INTO finance_schema.finance_data (
                            "id", "Date", "Year", "Month", "Day", "Quarter", "Weekday",
                            "Ticker", "Company", "Open", "High", "Low", "Close", "Volume", "Dividends", "stock_splits"
                        )
                        SELECT s.*
                        FROM finance_schema.{staging_table} s
                        WHERE NOT EXISTS (
                            SELECT 1 FROM finance_schema.finance_data f WHERE f."id" = s."id"
                        );
                    ''')

                    inserted_count = cursor.rowcount
                    conn.commit()
                    logging.info(f"{inserted_count} new records inserted into finance_data.")

                    # Drop the staging table after insert is complete
                    cursor.execute(f"DROP TABLE IF EXISTS finance_schema.{staging_table};")
                    conn.commit()
                    logging.info(f"Staging table {staging_table} dropped.")

                except Exception as e:
                    logging.error(f"Error inserting from staging to main table: {e}")
                    raise AirflowFailException("Insert or cleanup after staging failed.")
                finally:
                    cursor.close()
                    conn.close()

    except AnalysisException as e:
        logging.error(f"Spark AnalysisException: {e}")
        raise AirflowFailException(f"Spark AnalysisException: {e}")

    except Exception as e:
        logging.error(f"Unexpected error during data loading: {e}")
        raise AirflowFailException(f"Unhandled exception: {e}")

    logging.info("Data loading process completed successfully.")

