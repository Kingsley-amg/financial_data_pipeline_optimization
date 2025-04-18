def data_transformation():
    import logging
    import os
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import to_date, year, month, dayofmonth, quarter, date_format, col, monotonically_increasing_id
    from pyspark.sql.utils import AnalysisException
    from airflow.exceptions import AirflowFailException  # Ensures task failure is detected by Airflow

    # Set up logging to track processing steps and errors
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    try:
        # Initialize Spark session and configure it to handle legacy date parsing
        spark = SparkSession.builder \
            .appName("FinancialDataTransformation") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        logging.info("Spark session initialized successfully.")
    except Exception as e:
        logging.error(f"Spark initialization failed: {e}")
        raise AirflowFailException("Spark session could not be initialized.")

    try:
        # Define directories and file paths used in the transformation
        base_dir = "/home/airflow/airflow/financial_data_dag"
        
        raw_data_dir = os.path.join(base_dir, "raw_data")
        transformed_data_path = os.path.join(base_dir, "transformed_data")
        flag_file_path = os.path.join(raw_data_dir, "last_data_file.txt")

        # Ensure the flag file exists; it tells us which file to transform
        if not os.path.exists(flag_file_path):
            logging.error("Flag file last_data_file.txt not found.")
            raise AirflowFailException("Missing flag file indicating file to transform.")

        # Read the filename from the flag file (e.g., 'historical_data.csv' or 'current_data.csv')
        with open(flag_file_path, "r") as f:
            file_to_transform = f.read().strip()

        # Construct the absolute path to the CSV file that needs to be transformed
        data_path = os.path.join(raw_data_dir, file_to_transform)

        # Verify that the file exists
        if not os.path.exists(data_path):
            logging.error(f"Data file {file_to_transform} not found at expected path.")
            raise AirflowFailException(f"Missing data file: {file_to_transform}")

        # Load the CSV into a Spark DataFrame with schema inference
        df = spark.read.csv(data_path, header=True, inferSchema=True)

        # Check if the DataFrame is empty
        if df.count() == 0:
            logging.error(f"The file {file_to_transform} is empty.")
            raise AirflowFailException("Data file is empty. No transformation performed.")

        # Standardize column names and convert data types for consistency and downstream compatibility
        df = df \
            .withColumnRenamed("Stock Splits", "stock_splits") \
            .withColumn("Date", to_date(col("Date"))) \
            .withColumn("Open", col("Open").cast("double")) \
            .withColumn("High", col("High").cast("double")) \
            .withColumn("Low", col("Low").cast("double")) \
            .withColumn("Close", col("Close").cast("double")) \
            .withColumn("Volume", col("Volume").cast("long")) \
            .withColumn("Dividends", col("Dividends").cast("double")) \
            .withColumn("stock_splits", col("stock_splits").cast("double"))

        # Add time-derived columns for easier time-series analysis later
        derived_cols = {
            "Year": year(col("Date")),
            "Month": month(col("Date")),
            "Day": dayofmonth(col("Date")),
            "Quarter": quarter(col("Date")),
            "Weekday": date_format(col("Date"), "EEEE")
        }

        for name, expr in derived_cols.items():
            df = df.withColumn(name, expr)

        # Fill missing values based on their data types to prevent downstream issues
        for column, dtype in df.dtypes:
            if dtype in ['double', 'float']:
                df = df.fillna({column: 0.0})
            elif dtype in ['int', 'bigint']:
                df = df.fillna({column: 0})
            elif dtype == 'string':
                df = df.fillna({column: 'Unknown'})
            elif dtype == 'date':
                df = df.fillna({column: "1970-01-01"})

        # Add a unique ID column to each record and select the final list of columns to keep
        transformed_data = df.withColumn("id", monotonically_increasing_id()) \
            .select("id", "Date", "Year", "Month", "Day", "Quarter", "Weekday",
                    "Ticker", "Company", "Open", "High", "Low", "Close", "Volume", "Dividends", "stock_splits")

        # Save the transformed data to disk in Parquet format for efficient querying
        try:
            transformed_data.write.mode("overwrite").parquet(transformed_data_path)
            logging.info(f"Transformed data written to: {transformed_data_path}")
        except Exception as e:
            logging.error(f"Failed to write transformed data to Parquet: {e}")
            raise AirflowFailException("Failed to save transformed data.")

        # If the file being processed was current_data.csv, delete it after transformation
        if file_to_transform == "current_data.csv":
            try:
                os.remove(data_path)
                logging.info(f"Deleted temporary file: {data_path}")
            except Exception as e:
                logging.error(f"Failed to delete processed file: {e}")
                raise AirflowFailException(f"Failed to delete file: {data_path}")

    except AnalysisException as ae:
        logging.error(f"Spark AnalysisException: {ae}")
        raise AirflowFailException(f"Spark AnalysisException: {ae}")

    except Exception as e:
        logging.error(f"Unexpected transformation error: {e}")
        raise AirflowFailException(f"Unhandled transformation error: {e}")

