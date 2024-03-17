import psycopg2
import psycopg2.extras as extras
from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine

class DataTransformation:
    def __init__(self, db_uri, table_name, chunksize=10000):
        self.db_uri = db_uri
        self.table_name = table_name
        self.chunksize = chunksize

    def transform_and_write_data(self):
        engine = create_engine(self.db_uri)
        with engine.connect() as conn:
            try:
                for chunk in pd.read_sql_table('government', conn, chunksize=self.chunksize):
                    chunk = chunk[chunk['series_id'].str.endswith('10') & (chunk['period'].str.slice(start=1).astype(int) <= 12)].copy()
                    chunk.loc[:, 'date'] = pd.to_datetime(chunk['year'].astype(str) + chunk['period'].str.slice(start=1), format='%Y%m').dt.strftime('%B %Y')
                    chunk.loc[:, 'valueInThousands'] = chunk['value']
                    chunk = chunk[['date', 'valueInThousands']].dropna()
                    chunk = chunk.reset_index(drop=True)
                    chunk.to_sql(self.table_name, conn, if_exists='append', index=False, method='multi')
                print(f"Successfully wrote the transformed data to {self.table_name}")
            except Exception as e:
                print(f"An error occurred: {e}")

    def filter_and_write_data_production(self, endswith, orgtable, chunksize=1000):
        engine = create_engine(self.db_uri)
        with engine.connect() as conn:
            try:
                for chunk in pd.read_sql_table(orgtable, conn, chunksize=chunksize):
                    # Filter the chunk to keep rows where 'series_id' does not end with '32'
                    filtered_chunk = chunk[~chunk['series_id'].str.endswith(str(endswith))]

                    # Write the filtered chunk to a new table
                    new_table_name = self.table_name + "_new"
                    filtered_chunk.to_sql(new_table_name, conn, if_exists='append', index=False, method='multi')

                print(f"Successfully wrote the filtered data to {new_table_name}")
            except Exception as e:
                print(f"An error occurred: {e}")
                

    def filter_allemployees_based_on_production(self, orgtable, basetable):
        engine = create_engine(self.db_uri)
        with engine.connect() as conn:
            try:
                # Read the 'production' table
                production = pd.read_sql_table(orgtable, conn)

                # Create a temporary column in 'production' with 'series_id' without the last two characters
                production['temp_series_id'] = production['series_id'].str.slice(stop=-2)

                for chunk in pd.read_sql_table(basetable, conn, chunksize=self.chunksize):
                    # Create a temporary column in 'chunk' with 'series_id' without the last two characters
                    chunk['temp_series_id'] = chunk['series_id'].str.slice(stop=-2)

                    # Filter the chunk to remove rows where 'temp_series_id' is in the 'production' table
                    chunk = chunk[~chunk['temp_series_id'].isin(production['temp_series_id'])].copy()

                    # Drop the temporary column from 'chunk'
                    chunk.drop(columns=['temp_series_id'], inplace=True)

                    # Write the filtered chunk to the new table
                    chunk.to_sql(self.table_name+'_new', conn, if_exists='append', index=False, method='multi')

                print(f"Successfully wrote the filtered data to {self.table_name}")
            except Exception as e:
                print(f"An error occurred: {e}")





