import sys
import os
import time

import pandas as pd
from sqlalchemy.exc import OperationalError

from home_messages_db import *
import utils as utils

overall_duration = time.time()

# specify data path
smartthings_data_path = 'data/smartthings/'


##### COMMAND LINE #####
# communication with command line 
argv = sys.argv

if argv[1] in ['-h', '--help']:
    print("""
    Usage:
    \tsmartthings.py [OPTIONS] smartthingsLog.1.tsv [smartthingsLog.2.tsv...]

    Output options:
    \t-d DBURL insert into the project database (DBURL is a SQLAlchemy database URL)
    """)
    requested_files = []
elif argv[1] in ['-d']:
    db_url = argv[2]
    requested_files = argv[3:]
else:
    raise Exception('Unknown command line flag. Please use -h or --help for instructions.')


##### DATABASE OBJECT #####
# Initialize database object
try:
    db = home_messages_db(url = db_url)
except:
    sys.exit('DBURL does not work. Did you specify the name of the database correctly?')
# Check whether we can send a query
try:
    db.query_df('SELECT * FROM sources')
except OperationalError:
    raise Exception('Something went wrong. Communication with the database is not possible. Did you initialize the database?\n\nCheck the README file to see how to create an empty database first.')
    

##### GET FILES #####
# check whether given files exist or not
available_files = os.listdir(smartthings_data_path)
unknown_files = set(requested_files)-set(available_files)

if len(unknown_files) > 0:

    # if keyword for all files are given, set requested_files to all available data files.
    if 'smartthingsLog.*.tsv' in unknown_files:
        print(f'smartthingsLog.*.tsv has been given, so all available files are considered.')
        requested_files = available_files

    # check whether a certain modified filename is available (':' instead of '_')
    # otherwise through Exception
    else:
        for filename in unknown_files:
            if len(filename) > 31:
                filename = list(filename)
                filename[28] = '_'
                filename[31] = '_'
                filename = ''.join(filename)
                if filename in available_files:
                    sys.exit(f'Unknown file. Did you mean filename {filename} ?')
        raise Exception(f'Unknown file(s) {unknown_files}.')


##### READ AND STOR FILES #####
# read the specified files and store in database
for file in requested_files:

    df = pd.read_csv(os.path.join(smartthings_data_path, file), sep = '\t')
    df.rename(columns={'deviceId': 'device_id', 'deviceLabel': 'label'}, inplace = True)

    df['time'] = ( df['epoch'].apply(pd.Timestamp)- pd.Timestamp("1970-01-01", tz='UTC')) // pd.Timedelta('1s')

    source_id = utils.store_file(file, db)
    if source_id:
        begin = time.time()

        # insert data from file in all tables
        amount_inserted = []
        for table in utils.tables:
            inserted = utils.store_in_table(df, table, db, source_id)
            amount_inserted.append(inserted)

        print('Considered', file, '; Duration:', round(time.time()-begin, 3))
        print('Newly inserted rows in database table:', ' | '. join( [f' {t}: {i} ' for (t,i) in zip(utils.tables, amount_inserted)]), '\n')
    
    else:
        print(f'Did not consider {file}, since it already is stored in the database.')


print(db.query_df('SELECT count(*) FROM messages'))