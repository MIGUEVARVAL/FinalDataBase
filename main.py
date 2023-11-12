# Description: Main file to create the data warehouse and execute the ETL process.

import os
import pandas as pd
from Components import db
from Components import load

# Define paths

data_path = 'golden_stage'
db_path='DB'

# Variables

db_name = 'presupuesto.db'

# Execution
if __name__ == '__main__':
    # Read the data from the csv file
    i=3
    df = pd.read_excel(os.path.join(data_path, 'gastos_info.xlsx'))
    # Create the data warehouse
    db.create_db(db_name,db_path)
    # Insert the data into the data warehouse
    load.insert_data(df,db_name, db_path,i)
