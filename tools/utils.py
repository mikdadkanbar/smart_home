import pandas as pd
import datetime

from home_messages_db import *

table_df_colums = {
    'devices': ['device_id', 'label', 'location'],
    'unitcap': ['capability', 'attribute', 'unit'],
    'messages': ['time','attribute', 'value', 'device_id']
}

tables = table_df_colums.keys()

read_data = lambda df, tablename: df[table_df_colums[tablename]].drop_duplicates()




# Used in smartthings tools
def store_file(filename, db):
    """Takes the filename and the database.
    Inserts the filename to the Sources table if the file was not considered yet. It will then return the associated source_id.
    Returns None if the file is already stored in the source table."""

    # check whether the filename is already in the database Source table
    query = f"""SELECT source_id FROM sources WHERE file_name = '{filename}'"""
    result = db.query_df(query)
    #result = pd.DataFrame()

    if not result.empty:
        # file is already in database
        return None
    else:
        # file is not yet in the database
        type_file = '.'.split(filename)[-1]

        # create the new entry in the database
        db.addNewEntry( newEntry = Sources(
            #source_id = source_id,
            file_name = filename,
            type_file = type_file) 
            )
    
        query = f"""SELECT source_id FROM sources WHERE file_name = '{filename}'"""
        result = db.query_df(query)
        source_id = result.loc[0].item()

        return source_id


# Used in every tools
def remove_duplicates(inserting_df, database_df, tablename):
    """Removes all rows from the inserting_df that are already in the database_df"""
    
    if tablename == 'messages':
        database_df['time'] = database_df['time'].astype(str).astype(int)
        database_df['attribute'] = database_df['attribute'].astype(str)
        database_df['value'] = database_df['value'].astype(str)
        database_df['device_id'] = database_df['device_id'].astype(str)

        inserting_df['time'] = inserting_df['time'].astype(str).astype(int)
        inserting_df['attribute'] = inserting_df['attribute'].astype(str)
        inserting_df['value'] = inserting_df['value'].astype(str)
        inserting_df['device_id'] = inserting_df['device_id'].astype(str)
    
    if tablename == 'Consumption':
        database_df['time'] = database_df['time'].astype(str).astype(int)
        database_df['type_time'] = database_df['type_time'].astype(str)
        database_df['value'] = database_df['value'].astype(str)
        database_df['source_id'] = database_df['source_id'].astype(str)
        
        inserting_df['time'] = inserting_df['time'].astype(str).astype(int)
        inserting_df['type_time'] = inserting_df['type_time'].astype(str)
        inserting_df['value'] = inserting_df['value'].astype(str)
        inserting_df['source_id'] = inserting_df['source_id'].astype(str)
    
    # only keep the rows of the inserting_df that do not appear in the database_df
    # code from from https://stackoverflow.com/questions/44706485/how-to-remove-rows-in-a-pandas-dataframe-if-the-same-row-exists-in-another-dataf
    df = pd.merge(inserting_df,database_df, indicator=True, how='outer')
    df = df.query('_merge=="left_only"')
    df = df.drop('_merge', axis=1)
    
    return df

#Used in smartthings tools
def get_new_db_entries(db, subset_df, tablename):
    """Remove duplicates from the subset_df that are already stored in the database."""
    
    # write query to get a table to remove possible duplicates
    sql = f"SELECT * FROM {tablename}"

    if tablename == 'messages':
        min = subset_df.time.min()
        max = subset_df.time.max()
        sql = sql + f' WHERE time >= {min} and time <= {max}'

    # get the table
    db_df = db.query_df(sql)

    if tablename == 'messages':
        db_df.drop(columns=['source_id'])
        if 'source_id' in subset_df.columns:
            subset_df.drop(columns=['source_id'])

        df = remove_duplicates(subset_df[table_df_colums['messages']], db_df[table_df_colums['messages']], tablename)
    else:
        df = remove_duplicates(subset_df, db_df, tablename)

    return df


#smartthings
def store_in_table(df, tablename, db, source_id):
    """ Takes a DataFrame read from a file and stores the information relevant for the specified table in the given database.
    Returns the amount of newly inserted rows.
    """
    # subset dataframe to relevant information
    df_subset = read_data(df, tablename)

    # remove duplicates
    df_subset = get_new_db_entries(db, df_subset, tablename)

    # create a new column to store the associated source_id for each message
    if tablename == 'messages':
        df_subset['source_id'] = [source_id]*df_subset.shape[0]

    # insert data in database
    db.insert_df(df_subset, tablename)

    return df_subset.shape[0]

#Used in p1e and p1g tools
def get_time_type(row):
    """ Give you the type of time slot (low or high cost) for an observation/row
    Low cost hours : during nights and weekends
    """
    date_hour = datetime.datetime.strptime(row['time'], "%Y-%m-%d %H:%M")
    is_weekend = date_hour.weekday() in [5, 6] 
    is_night = (date_hour.time() >= datetime.time(20, 45)) or (date_hour.time() <= datetime.time(6, 45))
    if is_weekend or is_night:
        return 'low-cost'
    else:
        return 'high-cost'

# Used in p1e tools
def store_p1e_in_Consumption(df,file_id,db):
    """ Stores the P1e data in the Consumption table of the database.
    """ 
    # Select the needed variables and transform the cumulative variable to one "instantenous" variable and drop duplicates
    electricity_data = df[['time', 'Electricity imported T1', 'Electricity imported T2']].drop_duplicates()
    electricity_data = electricity_data.assign(value=electricity_data["Electricity imported T1"].diff()+electricity_data["Electricity imported T2"].diff()).dropna()
    electricity_data=electricity_data.drop(df.columns[[1, 2]], axis=1)
    # Create the  type_time,type and source_id column 
    electricity_data['type_time'] = electricity_data.apply(get_time_type, axis=1)
    electricity_data['type']='electricity'
    electricity_data['source_id']=file_id
    # Changing the time format to the Unix format
    electricity_data['time']=pd.to_datetime(electricity_data['time']).astype("int64") // 10**9
    #remove duplicates that are already in the db
    sql = f"SELECT * FROM Consumption"
    df_db=db.query_df(sql)
    electricity_data=remove_duplicates(electricity_data,df_db,"Consumption")
    # Insert
    db.insert_df(electricity_data, tablename="Consumption")
    

# Used in p1g tools

def store_p1g_in_Consumption(df, file_id,db):
    """ Takes a dataframe and stores the P1e data in the Consumption table of the given database.
    """ 
    # Select the needed variables and transform the cumulative variable to one "instantenous" variable
    gas_data = df.drop_duplicates().assign(value=df["Total gas used"].diff()).dropna().drop("Total gas used",axis=1)
    # Create the  type_time,type and source_id column 
    gas_data['type']='gas'
    gas_data['source_id']=file_id
    gas_data['type_time'] = gas_data.apply(get_time_type, axis=1)
    # Changing the time format to the Unix format
    gas_data['time'] =pd.to_datetime(gas_data['time']).astype("int64") // 10**9
    #remove duplicates that are already in the db 
    sql = f"SELECT * FROM Consumption"
    df_db=db.query_df(sql)
    gas_data=remove_duplicates(gas_data,df_db,"Consumption")
    # Insert
    db.insert_df(gas_data, tablename="Consumption")
    
# p1e and p1g tools
def store_in_source(filename,db):
    "Stores source informations in the table Sources in the the database and return the id"
    try : 
        db.addNewEntry(newEntry=Sources(file_name = filename,type_file = "csv"))
    except: 
        print("The data hasn't be stored:", str(e))
    query=f"SELECT source_id FROM sources WHERE file_name = '{filename}'"
    source_id=db.query_df(query).at[0,"source_id"]
    return source_id