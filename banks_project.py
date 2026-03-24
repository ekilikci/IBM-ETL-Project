import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
import logging
from datetime import datetime



# the project requirements and configurations
data_URL= 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
table_attributes_initial = ['Name', 'MC_USD_Billion']
table_attributes_final = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
output_csv_path = './Largest_banks_data.csv'
database_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'

def log_process(message):
    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s : %(message)s')
    logging.info(message)

def extract(url, table_attributes=table_attributes_initial):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    log_process("Preliminaries complete. Initiating ETL process")
    response = requests.get(url)
    response.raise_for_status()

    data = BeautifulSoup(response.text, 'html.parser')
    rows = data.find_all("tbody")[0].find_all("tr")
    
    extracted_data = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) >= 2:
            name = cols[1].text.strip()
            mc_usd = cols[2].text.strip()
            extracted_data.append((name, mc_usd))
    log_process("Data extraction complete. Initiating Transformation process")
    return pd.DataFrame(extracted_data, columns=table_attributes)

def transform(df, csv_path):
    """
    This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies
    """
    
    opening_csv = pd.read_csv(csv_path)
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)  # Ensure the market cap is in float
    df['MC_GBP_Billion'] = df['MC_USD_Billion'] * opening_csv.loc[opening_csv['Currency'] == 'GBP', 'Rate'].values[0]
    df['MC_EUR_Billion'] = df['MC_USD_Billion'] * opening_csv.loc[opening_csv['Currency'] == 'EUR', 'Rate'].values[0]
    df['MC_INR_Billion'] = df['MC_USD_Billion'] * opening_csv.loc[opening_csv['Currency'] == 'INR', 'Rate'].values[0]
    
    log_process("Data transformation complete. Initiating Loading process")
    return df
    
def load_to_csv(df, output_csv_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    
    df.to_csv(output_csv_path, index=False)
    log_process("Data saved to CSV file")

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    log_process("SQL Connection initiated")
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False) 
    log_process("Data loaded to Database as a table, Executing queries")

def run_queries(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    log_process("Process Complete")
    print(query_statement )
    result = pd.read_sql(query_statement, sql_connection)
    print(result)
    log_process("Server Connection closed")

df = extract(data_URL, table_attributes_initial)
transform(df,csv_path)
load_to_csv(df, output_csv_path)
sql_connection = sqlite3.connect(database_name)
load_to_db(df, sql_connection, table_name)
query_statement = f"SELECT * from {table_name}"
run_queries(query_statement, sql_connection)
query_statement= f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_queries(query_statement, sql_connection)
query_statement= f"SELECT Name from {table_name} LIMIT 5"
run_queries(query_statement, sql_connection)

sql_connection.close()
