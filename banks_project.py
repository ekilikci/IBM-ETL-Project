import logging
import sqlite3

import pandas as pd
import requests
from bs4 import BeautifulSoup


# Project requirements and configurations
DATA_URL = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
CSV_PATH = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"

TABLE_ATTRIBUTES_INITIAL = ["Name", "MC_USD_Billion"]
TABLE_ATTRIBUTES_FINAL = [
    "Name",
    "MC_USD_Billion",
    "MC_GBP_Billion",
    "MC_EUR_Billion",
    "MC_INR_Billion",
]

OUTPUT_CSV_PATH = "./Largest_banks_data.csv"
DATABASE_NAME = "Banks.db"
TABLE_NAME = "Largest_banks"
LOG_FILE = "code_log.txt"


# Configure logging once
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s : %(levelname)s : %(message)s",
)


def log_process(message: str) -> None:
    """Write a message to the log file."""
    logging.info(message)


def extract(url: str, table_attributes: list[str] = TABLE_ATTRIBUTES_INITIAL) -> pd.DataFrame:
    """
    Extract the required bank market cap information from the website
    and return it as a DataFrame.
    """
    log_process("Starting data extraction.")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        logging.exception("Failed to fetch data from source URL.")
        raise RuntimeError(f"Failed to fetch data from URL: {url}") from exc

    soup = BeautifulSoup(response.text, "html.parser")
    tables = soup.find_all("tbody")

    if not tables:
        logging.error("No <tbody> elements found in the HTML.")
        raise ValueError("Could not locate the required table in the HTML page.")

    rows = tables[0].find_all("tr")
    extracted_data: list[tuple[str, float]] = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 3:
            continue

        name = cols[1].get_text(strip=True)
        mc_usd_raw = cols[2].get_text(strip=True)

        # Clean market cap text
        mc_usd_clean = (
            mc_usd_raw.replace(",", "")
            .replace("\n", "")
            .replace("[", "")
            .replace("]", "")
            .strip()
        )

        try:
            mc_usd_value = float(mc_usd_clean)
        except ValueError:
            logging.warning(
                "Skipping row due to invalid market cap value. Name=%s, RawValue=%s",
                name,
                mc_usd_raw,
            )
            continue

        extracted_data.append((name, mc_usd_value))

    df = pd.DataFrame(extracted_data, columns=table_attributes)
    log_process(f"Data extraction complete. Extracted {len(df)} rows.")
    return df


def transform(df: pd.DataFrame, csv_path: str) -> pd.DataFrame:
    """
    Read exchange rates and add transformed market cap columns
    in GBP, EUR, and INR.
    """
    log_process("Starting data transformation.")

    try:
        exchange_rates = pd.read_csv(csv_path)
    except Exception as exc:
        logging.exception("Failed to read exchange rate CSV.")
        raise RuntimeError(f"Failed to read exchange rates from: {csv_path}") from exc

    required_currencies = {"GBP", "EUR", "INR"}
    available_currencies = set(exchange_rates["Currency"].unique())

    missing = required_currencies - available_currencies
    if missing:
        logging.error("Missing required currencies in exchange rate file: %s", missing)
        raise ValueError(f"Missing currencies in exchange rate file: {missing}")

    rates = exchange_rates.set_index("Currency")["Rate"].to_dict()

    result = df.copy()
    result["MC_USD_Billion"] = pd.to_numeric(result["MC_USD_Billion"], errors="coerce")
    result = result.dropna(subset=["MC_USD_Billion"])

    result["MC_GBP_Billion"] = (result["MC_USD_Billion"] * rates["GBP"]).round(2)
    result["MC_EUR_Billion"] = (result["MC_USD_Billion"] * rates["EUR"]).round(2)
    result["MC_INR_Billion"] = (result["MC_USD_Billion"] * rates["INR"]).round(2)

    result = result[TABLE_ATTRIBUTES_FINAL]

    log_process("Data transformation complete.")
    return result


def load_to_csv(df: pd.DataFrame, output_csv_path: str) -> None:
    """
    Save the final DataFrame as a CSV file.
    """
    log_process(f"Saving data to CSV: {output_csv_path}")
    try:
        df.to_csv(output_csv_path, index=False)
    except Exception as exc:
        logging.exception("Failed to save data to CSV.")
        raise RuntimeError(f"Failed to save CSV to: {output_csv_path}") from exc

    log_process("Data saved to CSV successfully.")


def load_to_db(df: pd.DataFrame, sql_connection: sqlite3.Connection, table_name: str) -> None:
    """
    Save the DataFrame to a SQLite database table.
    """
    log_process(f"Loading data into database table: {table_name}")
    try:
        df.to_sql(table_name, sql_connection, if_exists="replace", index=False)
    except Exception as exc:
        logging.exception("Failed to load data into database.")
        raise RuntimeError(f"Failed to load data into table: {table_name}") from exc

    log_process("Data loaded to database successfully.")


def run_queries(query_statement: str, sql_connection: sqlite3.Connection) -> pd.DataFrame:
    """
    Run the query on the database table, print the result,
    and return the result DataFrame.
    """
    log_process(f"Running query: {query_statement}")
    try:
        result = pd.read_sql(query_statement, sql_connection)
    except Exception as exc:
        logging.exception("Failed to execute query.")
        raise RuntimeError(f"Failed to execute query: {query_statement}") from exc

    print(f"\nQuery: {query_statement}")
    print(result)

    log_process("Query executed successfully.")
    return result


def main() -> None:
    """Run the ETL pipeline."""
    log_process("ETL process started.")
    sql_connection = None

    try:
        df = extract(DATA_URL, TABLE_ATTRIBUTES_INITIAL)
        df = transform(df, CSV_PATH)

        load_to_csv(df, OUTPUT_CSV_PATH)

        sql_connection = sqlite3.connect(DATABASE_NAME)
        log_process(f"Database connection established: {DATABASE_NAME}")

        load_to_db(df, sql_connection, TABLE_NAME)

        run_queries(f"SELECT * FROM {TABLE_NAME}", sql_connection)
        run_queries(f"SELECT AVG(MC_GBP_Billion) AS AVG_MC_GBP_Billion FROM {TABLE_NAME}", sql_connection)
        run_queries(f"SELECT Name FROM {TABLE_NAME} LIMIT 5", sql_connection)

        log_process("ETL process completed successfully.")

    except Exception as exc:
        logging.exception("ETL process failed.")
        print(f"ETL process failed: {exc}")

    finally:
        if sql_connection is not None:
            sql_connection.close()
            log_process("Database connection closed.")


if __name__ == "__main__":
    main()