import pandas as pd
import config
import etl_flow

"""
load_sql.py functions
These functions are for pulling, updating, and inserting values into the specified data tables.
"""
def get_table(db, name):
    """"
    Gets specified table from specified database using the user-edited config file
    """
    engine = etl_flow.get_engine(config.postgresql, db)
    query = "SELECT * FROM {0}".format(name)
    table = pd.read_sql(query, con=engine)
    return table

def update_table(staging, new):
    """Updates records in main table using most-recent data export."""
    engine = etl_flow.get_engine(config.postgresql, "testdb")
    current_table = get_table("testdb", "cme_all_staging")
    set_cols = "SET " + str(tuple([x + ' = ' + new + "." + x for x in list(current_table.columns.values)])).replace('(',
                                                                                                           '').replace(
        ')', '').replace(
        "'", '')
    query = "UPDATE {0} {1} FROM {2} WHERE {0}.uid = {2}.uid".format(staging, set_cols, new)
    with engine.connect() as con:
        con.execute(query)
        con.close()
    engine.dispose()

def insert_table(staging, new):
    """Inserts values into table where the trade date isn't represented. Inserts any new values by date."""
    engine = etl_flow.get_engine(config.postgresql, "testdb")
    current_table = get_table("testdb", "cme_all_staging")
    set_cols = str(tuple((current_table.columns.values))).replace("'", '"')
    query = "INSERT INTO {0}{1} SELECT *  FROM {2} WHERE trade_date NOT IN (SELECT trade_date FROM {0});".format(staging, set_cols, new)
    with engine.connect() as con:
        con.execute(query)
        con.close()
    engine.dispose()


if __name__ == "__main__":
    etl_flow.setup_table("testdb", "cme", 'uid')
    etl_flow.load_table("testdb", "cme_all_staging", 'uid')
    insert_table("cme","cme_all_staging")
    update_table("cme","cme_all_staging")
