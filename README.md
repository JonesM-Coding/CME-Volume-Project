# CME-Volume-Project
This project is to develop an ETL script for pulling and processing Daily Exchange Volume and Open Interest (VOI) from CME Group.

The project is meant to address the following criteria:
ETL has to be able to run multiple times at will.
• Backfill function that can load historical data for date range of preference has to be provided.
• Backfill should not change or intersect by any means with ETL logic (keep it separated).
• ETL has to be broken down into 3 main functions: Extract, Transform and Load.
• Data has to be transformed to fit in one table.
• DB interaction logic as a separate script is preferable. Main script can just import and run it.
• Loading logic has to prevent duplicates from inserting by overwriting data (on top of DB table
PKs).
• Data has to have TradeDate included.
• All fields has to have proper data format.
• DB Table must have appropriate Primary Keys
• DB credentials should be stored in a config file for an easy switch.

https://www.cmegroup.com/market-data/volume-open-interest/exchange-volume.html

There are two main scripts for the process:
etl_flow: Contains all functions needed for the ETL process.
load_sql: Contains all SQL connections and queries. etl_flow functions are included in this script.

Note: This project uses Selenium and the chromedriver.exe file for user's version of crome is required. Download the appropriate driver at https://chromedriver.chromium.org/downloads
