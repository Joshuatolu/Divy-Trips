## Import necessary libraries
import pandas as pd
from datetime import datetime as dt
from sqlalchemy import text

def load_data(schema, engine):
    '''
    Loading the data to postgresql
    '''
    tripdata = pd.read_csv('dags\raw_files\tripdata.csv')

    ## Get columns with unsigned integers and transform them. These columns are not fit to enter postgreSQL. They should be converted to signed integers
    for cols in tripdata.select_dtypes(include=['uint64', 'uint8']).columns:
        tripdata[cols] = tripdata[cols].astype('int64')

    tripdata['load_datetime'] = pd.Timestamp('now')

    with engine.connect() as connection:
        # Get the unique date
        d_date = tripdata.pickup_date.unique()

        # Check for the dates in the DB that already exists in the dataframe
        date_check = f'''
        SELECT DISTINCT pickup_date
            FROM "{schema}".daily_trips
            WHERE pickup_date IN ({', '.join([f"'{date}'" for date in d_date])});
        '''

        # The fetchall() helps to return the actual values
        existing_dates = connection.execute(text(date_check)).fetchall()
        
        # If existing date exists
        if existing_dates:
            print("Duplicate record found. No data will be inserted")
            d_log = f'''
                    INSERT INTO "{schema}".loading_daily_trip_logg (STATUS, MESSAGE)
                    VALUES ('FAILED', 'Loading failed due to existing data');
            '''
            connection.execute(text(d_log))

        # If no existing date exists
        else:
            tripdata.to_sql('daily_trips', schema=schema, con=engine, index=False, if_exists='append')
            print(f'You have successfully loaded {len(tripdata)} records into the DB.')
            
            d_log = f'''
                    INSERT INTO "{schema}".loading_daily_trip_logg (STATUS, MESSAGE)
                    VALUES ('SUCCESS', 'Loading was successfull');
            '''
            connection.execute(text(d_log))

def move_data(engine):
    '''
        this function connection to the postgreSQL DB and runs a stored procedure
    '''
    with engine.connect() as connection:
        # Calling of the stored procedure
        sql_query = 'CALL "STG".PRC_AGG_TRIGGERS();'
        connection.execute(text(sql_query))

        # This is important in order to save the update from the stored procedure
        connection.commit()
    print("Stored Procedure ran!")
