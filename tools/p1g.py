from home_messages_db import *
from utils import *
import pandas as pd
import sys
import os


def store_p1g(file,db):
    p1g_data_path = 'data/P1g/'
    query = f"SELECT source_id FROM Sources WHERE file_name = '{file}'"
    result = db.query_df(query)
    #Check if the file has already been stored
    if result.empty:
        # Stores the file in source 
        df = pd.read_csv(os.path.join(p1g_data_path , file))
        source_id=store_in_source(file,db)
        try:
            store_p1g_in_Consumption(df,source_id,db)
            print('Loaded data from', file)
        except Exception as e:
            #If for some reasons the file hasn't been store
            #We delete the entry in source and raise an exception
            db.delete_entry("Sources","source_id",source_id)
            print("The file hasn't be stored:", str(e))
    else : 
        print('The file {} is already in the database'.format(file))

# Check the command line arguments passed to the script
argv = sys.argv
if argv[1] in ['-h', '--help']:
    print("""
    Usage:
    \tp1g.py [OPTIONS] P1g-2022-12-01-2023-01-10.csv.gz [...]

    Output options:
    \t-d DBURL insert into the project database (DBURL is a SQLAlchemy database URL)
    """)
    sys.exit()

# Get the database URL and the file names from the command line arguments
db_url = None
requested_files = []

if argv[1] in ['-d']:
    db_url = argv[2]
    requested_files = argv[3:]


# check whether given files exist or not
p1g_data_path = 'data/P1g/'
available_files = os.listdir(p1g_data_path)
unknown_files = set(requested_files)-set(available_files)

if len(unknown_files) > 0:
    if 'P1g-*.csv.gz' in unknown_files:
        print(f'data/P1g/P1g*.csv.gz has been given, so all available files are considered.')
        requested_files = available_files

    else:
        raise Exception(f'Unknown file(s) {unknown_files}.')


# Initialize the database object
try:
    db = home_messages_db(url=db_url)
except:
    sys.exit('DBURL does not work. Did you specify it correctly and does the database work?')

# Process the requested files
for file in requested_files:
    #Stores file in the database
    store_p1g(file,db)
