# Description: Main file to create the data warehouse and execute the ETL process.

import os
import pandas as pd
from Components import db
from Components import load

# Define paths

data_path = 'Components\golden_stage'
db_path='DB'

# Variables

db_name = 'tiendas.db'

# Execution
if __name__ == '__main__':
    # Read the data from the csv file
    df = pd.read_excel(os.path.join(data_path, 'Sales_Dataset_Input_Info.xlsx'))
    # Create the data 
    db.create_db(db_name,db_path)
    # Insert the data 
    load.insert_data(df,db_name, db_path)


