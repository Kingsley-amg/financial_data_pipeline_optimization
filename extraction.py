def data_extraction():
    import yfinance as yf
    import pandas as pd
    import os
    import logging
    from datetime import datetime, timedelta
    from dotenv import load_dotenv
    from airflow.exceptions import AirflowFailException 

    # Set this to True only for the first time to download full historical data
    is_first_run = True

    # Load any environment variables (e.g., paths, credentials)
    load_dotenv(override=True)

    # Configure logging to track all events and errors
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Define the list of stock tickers to extract data for
    tickers = [
        "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "NFLX", "BRK-B", "JPM",
        "V", "UNH", "PG", "JNJ", "MA", "DIS", "XOM", "BAC", "HD", "PFE"
    ]

    # Set file paths for saving the outputs
    output_dir = "/home/airflow/airflow/financial_data_dag/raw_data"
    os.makedirs(output_dir, exist_ok=True)  # Create directory if it doesn't exist

    historical_file = os.path.join(output_dir, 'historical_data.csv')  # Full data across runs
    current_file = os.path.join(output_dir, 'current_data.csv')        # Latest daily extract
    flag_file = os.path.join(output_dir, 'last_data_file.txt')         # Flag to tell next script what file to process

    try:
        # Store all extracted ticker data in a list of DataFrames
        dataframes = []

        # Loop through all stock tickers and fetch their data
        for ticker in tickers:
            try:
                if is_first_run:
                    # If this is the first run, download the full historical data
                    logging.info(f"[FIRST RUN] Fetching full historical data for {ticker}")
                    ticker_data = yf.Ticker(ticker).history(period='max')
                else:
                    # For subsequent runs, fetch only today's data
                    today = datetime.today().strftime('%Y-%m-%d')
                    tomorrow = (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d')
                    logging.info(f"[DAILY RUN] Fetching today's data for {ticker}")
                    ticker_data = yf.Ticker(ticker).history(start=today, end=tomorrow)

                # If no data is returned for this ticker, skip it
                if ticker_data.empty:
                    logging.warning(f"No data returned for {ticker}")
                    continue

                # Reset index so that 'Date' becomes a column instead of index
                ticker_data = ticker_data.reset_index()

                # Ensure the 'Date' column is timezone-naive for consistency
                ticker_data['Date'] = pd.to_datetime(ticker_data['Date'], utc=True).dt.tz_convert(None)

                # Add the ticker symbol as a new column to track source
                ticker_data['Ticker'] = ticker

                # Add to our collection of data
                dataframes.append(ticker_data)

            except Exception as e:
                # If a single ticker fails, raise an exception to halt the DAG
                logging.error(f"Error fetching data for {ticker}: {e}")
                raise AirflowFailException(f"Data fetch failed for {ticker}: {e}")

        # If none of the tickers returned data, fail the task
        if not dataframes:
            logging.error("No data was fetched for any ticker.")
            raise AirflowFailException("Data extraction failed: No data fetched.")

        # Combine all ticker data into one DataFrame
        current_data = pd.concat(dataframes, ignore_index=True)

        # Select relevant columns and maintain order
        current_data = current_data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits', 'Ticker']]

        # Map ticker codes to company names and add a 'Company' column
        ticker_to_company = {
            'AAPL': 'Apple Inc.', 'MSFT': 'Microsoft Corporation', 'GOOGL': 'Alphabet Inc.',
            'AMZN': 'Amazon.com, Inc.', 'TSLA': 'Tesla, Inc.', 'META': 'Meta Platforms, Inc.',
            'NVDA': 'NVIDIA Corporation', 'NFLX': 'Netflix, Inc.', 'BRK-B': 'Berkshire Hathaway Inc.',
            'JPM': 'JPMorgan Chase & Co.', 'V': 'Visa Inc.', 'UNH': 'UnitedHealth Group Incorporated',
            'PG': 'Procter & Gamble Co.', 'JNJ': 'Johnson & Johnson', 'MA': 'Mastercard Incorporated',
            'DIS': 'The Walt Disney Company', 'XOM': 'Exxon Mobil Corporation',
            'BAC': 'Bank of America Corporation', 'HD': 'The Home Depot, Inc.', 'PFE': 'Pfizer Inc.'
        }
        current_data['Company'] = current_data['Ticker'].map(ticker_to_company)

        # Decide how to update the historical file based on first run flag
        if is_first_run:
            # If first run, historical data is equal to current data
            historical_data = current_data.copy()
        else:
            if os.path.exists(historical_file):
                # Read existing historical file and append today's data
                existing_data = pd.read_csv(historical_file, parse_dates=['Date'])
                historical_data = pd.concat([existing_data, current_data], ignore_index=True)
                historical_data.drop_duplicates(subset=['Date', 'Ticker'], keep='last', inplace=True)
            else:
                # If file doesn't exist, create it anew
                logging.warning("No historical file found. Creating a new one.")
                historical_data = current_data.copy()

        # Sort historical data by ticker and date for better readability
        historical_data.sort_values(by=['Ticker', 'Date'], inplace=True)

        # Save the historical and current datasets to CSV
        try:
            historical_data.to_csv(historical_file, index=False)
            current_data.to_csv(current_file, index=False)
            logging.info(f"Current data saved to: {current_file}")
            logging.info(f"Historical data saved to: {historical_file}")
        except Exception as e:
            logging.error(f"Error saving CSV files: {e}")
            raise AirflowFailException("Failed to write CSV files to disk.")

        # Write the last file indicator for the transformation script
        try:
            with open(flag_file, "w") as f:
                f.write("historical_data.csv" if is_first_run else "current_data.csv")
            logging.info(f"File to transform written to flag: {flag_file}")
        except Exception as e:
            logging.error(f"Failed to write flag file: {e}")
            raise AirflowFailException("Failed to write last_data_file.txt")

    except Exception as e:
        # Catch all other unexpected errors and raise them to stop the DAG
        logging.error(f"Uncaught exception during data extraction: {e}")
        raise AirflowFailException(f"Uncaught exception: {e}")

