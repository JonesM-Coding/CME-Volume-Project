import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import numpy as np
import time
import config
import sqlalchemy
from sqlalchemy_utils import database_exists, create_database

"""
etl_flow.py functions

This .py file scrapes tables for all available dates at 
https://www.cmegroup.com/market-data/volume-open-interest/exchange-volume.html.
Citation:
CME Group (2022). Daily Exchange Volume and Open Interest: VOI by Exchange
"""
# Press Shift+F10 to execute it or replace it with your code.
# # Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
def setup_driver():
    """Helper function to establish selenium Chrome Driver"""
    driver_path = "./driver/chromedriver.exe"
    opts = webdriver.ChromeOptions()
    opts.add_argument("--headless")
    opts.add_argument("user-agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36'")
    opts.add_argument("--start-maximized")
    prefs = {"profile.default_content_settings.popups": 0,
             # IMPORTANT - ENDING SLASH V IMPORTANT
             "directory_upgrade": True}
    opts.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(executable_path=driver_path, options=opts)
    return driver

def get_cme_dates():
    """Gets all available Trade Dates and URL's for any main url on the main url page"""
    driver = setup_driver()
    driver.get(r'https://www.cmegroup.com/market-data/volume-open-interest/exchange-volume.html')
    page = driver.page_source #Selenium version of the web page
    page_bs4 = BeautifulSoup(page, 'html.parser') #BeautifulSoup version of the web page

    #Finds Trade Date dropdown on page and stores elements in list
    trade_dates = page_bs4.find(id="tradedate").findChildren("option")

    #Parses out Trades Date into Pandas DataFrame for pulling the functions
    trade_table = pd.DataFrame([(x['value'], x.text) for x in trade_dates], columns=["TradeDate", "TradeText"])
    trade_table['Version'] = np.where(trade_table['TradeText'].str.contains("Preliminary"), "P", "F")

    #Pulls all possible searchable urls that exist on the main page (not in use)
    links = page_bs4.find("ul", class_="cmeHorizontalList cmeListSeparator")
    links_list = links.find_all("a", class_="none")
    links_table = pd.DataFrame([(x.text.replace("\n", ""), "https://www.cmegroup.com" + x['href']) for x in links_list],
                               columns=["Table", "URL"])

    #Appends the main page information to do a search on multiple URL's (not in use)
    links_table= links_table.append(dict(
        Table="VOI By Exchange",
        URL='https://www.cmegroup.com/market-data/volume-open-interest/exchange-volume.html'
    ), ignore_index=True)
    links_table = links_table.sort_values(by="Table", ascending=False).reset_index(drop=True)

    return trade_table, links_table

def getDownload(url, trades_table):
    """Downloads tables for all possible dates at the URL argument and formats the data with Trade Dates and Versions"""
    #Opens url webpage
    driver = setup_driver()
    driver.get(url)

    all_dates = [] # List for passing in processed dataframes

    #Iterates through all dates captured in the get_cme_dates() function
    for index, row in trades_table.iterrows():
        #Accesses the webpage dropdown to click through each available tradedate
        select = Select(driver.find_element(By.ID, "tradesDropdown"))
        select.select_by_index(index)
        time.sleep(1)
        #Creates a Selenium and BeautifulSoup version of the html on the page
        page = driver.page_source
        page_bs4 = BeautifulSoup(page, 'html.parser')

        #Converts all tables on the page into a list of unformatted dataframes
        tables = pd.read_html(page)

        #Gets main header for each table and stores value into a list
        table_names = [x.text for x in page_bs4.find(id="loadTable").findChildren("h3")]

        #While loop works though each dataframe and header, appending Trade Date information and versions to data
        int=0
        while int<= len(table_names)-1:
            #Formats columns and adds Trade Date information for each table
            tables[int].columns = ["_".join(col).lower().replace(" ", "_").replace("/", "_").replace("-", "_") for col in
                                   tables[int].columns.values]
            tables[int]["table_name"] = table_names[int]
            tables[int] = tables[int].rename(columns={"___": "category_full"})
            tables[int]["trade_date"] = row['TradeDate']
            tables[int]["trade_text"] = row['TradeText']
            tables[int]["version"] = row['Version']
            int+=1
        #Merges tables together in one dataframe
        main_table = pd.concat(tables, axis=0, sort=False)
        #Appends processed table for selected trade date to empty list
        all_dates.append(main_table)

    #Merges all collected and identified Trade Dates into one final list
    final_table = pd.concat(all_dates, axis=0, sort=False)
    return final_table


def get_cme_tables():
    """Final function for organizing data and pulling available dates"""
    dates, links = get_cme_dates()
    table = getDownload("https://www.cmegroup.com/market-data/volume-open-interest/exchange-volume.html", dates)
    table.to_csv("check.csv", index=False)
    return table

def format_cme():
    """Functions to format data and setup PK's"""
    table = get_cme_tables()
    remap_category = {'exchange': 'exc',
             'exchange futures':'exc_fut',
             'exchange options':'exc_opt',
             'OTC Cleared-Only Forward Swaps':'otc',
             'agriculture':'agr',
             'energy':'ene',
             'equities':'equ',
             'FX':'fx',
             'interest rate':'int_rate',
             'metals':'met',
             'CBOT Division':'cbot',
             'CME Division':'cme',
             'COMEX Division':'comex',
             'GEM Division':'gem',
             'IMM Division':'imm',
             'IOM Division':'iom',
             'NYMEX Division':'nymex'
    }
    remap_tablename = {'Futures, Options & Forwards':'fof',
                       'Futures Only':'fut',
                       'Options Only':'opt',
                       'Forward Swaps Only':'fs',
                       'Options Forward Swaps':'ofs',
                       'Divisions':'',
                       'Exchange':''
                       }
    table['category'] = table['category_full'].map(remap_category)
    table['table_trunc'] = table['table_name'].map(remap_tablename)

    #UID is a text-based PK based on date, table category, and truncated table name
    table['uid'] = table['trade_date'] + "_" + table['category'] + "_" + table['table_trunc']
    table.set_index('uid', inplace=True)
    table['trade_date'] = table['trade_date'].apply(pd.to_datetime)
    return table

def get_engine(config_file, db):
    """
    Parses out entries in the config file to connect to the database and creates sql engine
    """
    url = f"{config_file['program']}://{config_file['user']}:{config_file['passwd']}@{config_file['host']}:{config_file['port']}/{db}"
    if not database_exists(url):
        create_database(url)
    engine = sqlalchemy.create_engine(url, pool_size=50, echo=False)
    return engine

def setup_table(db, tablename, column):
    """
    Sets up initial table with data types of table doesn't already exist in database
    """
    engine = get_engine(config.postgresql, db)
    if sqlalchemy.inspect(engine).has_table(tablename) == True:
        print("{0} currently exists in {1}.".format(tablename, db))
    else:
        print("{0} does not exist. Loading data into {1}".format(tablename, db))
        table = format_cme().head(0)
        table.to_sql(con=engine, name=tablename, method='multi')
        with engine.connect() as con:
            con.execute('ALTER TABLE {0} ADD PRIMARY KEY ("{1}");'.format(tablename, column))
        print("{0} has been loaded into {1}".format(tablename, db))

def load_table(db, tablename, column):
    """Loads table into excel using pandas.to_sql (Slow, but preserves data types)"""
    engine = get_engine(config.postgresql, db)
    print("Loading data into {0}".format(tablename))
    table = format_cme()
    table.to_sql(con=engine, name=tablename, method='multi', if_exists='replace')
    with engine.connect() as con:
        con.execute('ALTER TABLE {0} ADD PRIMARY KEY ("{1}");'.format(tablename, column))
    print("{0} has been loaded into {1}".format(tablename, db))