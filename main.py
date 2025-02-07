## Import custom functions
from Modules.extract import extract_data
from Modules.helper import connect_clickhouse, connect_postgresql
from Modules.load import load_data, move_data
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

engine = connect_clickhouse()
db_engine = connect_postgresql()

Session = sessionmaker(bind=db_engine)
session = Session()
result = session.execute(text('select max(pickup_date) from "STG".daily_trips'))
max_date = result.fetchone()[0]
session.close()

sql_query = f"""
            SELECT * FROM tripdata 
            WHERE pickup_date = toDate('{max_date}') + 1
            """

def main():
    extract_data(sql_query, engine)

    load_data("STG", db_engine)

    move_data(db_engine)

if __name__ == '__main__':
    main()

