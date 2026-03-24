import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
import logging



# Definitions
data_URL= 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_URL = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
table_attributes_initial = ['Name', 'MC_USD_Billion']
table_attributes_final = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
output_csv_path = './Largest_banks_data.csv'
database_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'
