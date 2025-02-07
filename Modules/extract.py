## Function to extract data from Click_House cloud

import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

def extract_data(sql, client):
    tripdata = client.query_df(sql)
    tripdata.to_csv('dags\raw_files\tripdata.csv')
    print(f'You have successfully extracted {len(tripdata)} from Click House.')


def get_last_loaded_date(db_engine):
    Session = sessionmaker(bind=db_engine)
    session = Session()
    result = session.execute(text('select max(pickup_date) from "STG".daily_trips'))
    max_date = result.fetchone()[0]
    session.close()
    return max_date