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

    def transform_and_write_data_production(self):
        engine = create_engine(self.db_uri)
        with engine.connect() as conn:
            try:
                for chunk in pd.read_sql_table('productionemployees', conn, chunksize=self.chunksize):
                    # Print column names
                    print(chunk.columns)

                    # Print column names
                    print(chunk.columns)

                    # Filter the chunk to remove rows where 'series_id' ends with '32'e
                    if 'footnote_cods_m5140' in chunk.columns:
                        chunk = chunk[~chunk['series_id'].str.endswith('32')].copy()

                    # Replace None values in 'footnote_codes_m5140' with a default value
                    if 'footnote_codes_m5140' in chunk.columns:
                        chunk['footnote_codes_m5140'] = chunk['footnote_codes_m5140'].fillna('')

                    # Truncate all string columns to a maximum length of 256 characters
                    for col in chunk.select_dtypes(include=[object]):
                        chunk[col] = chunk[col].str.slice(0, 256)

                    # Write the filtered chunk to the new table
                    chunk.to_sql(self.table_name, conn, if_exists='append', index=False, method='multi')

                print(f"Successfully wrote the filtered data to {self.table_name}")
            except Exception as e:
                print(f"An error occurred: {e}")

    def filter_allemployees_based_on_production(self):
        engine = create_engine(self.db_uri)
        with engine.connect() as conn:
            try:
                # Read the 'production' table
                production = pd.read_sql_table('production', conn)

                # Print the columns of the 'production' table
                print("Production columns:", production.columns)

                # Remove the last two characters from the 'series_id' column in the 'production' table
                production['series_id'] = production['series_id'].str.slice(stop=-2)

                for chunk in pd.read_sql_table('allemployees', conn, chunksize=self.chunksize):
                    # Print the columns of the 'allemployees' table
                    print("Allemployees columns:", chunk.columns)

                    # Remove the last two characters from the 'series_id' column in the 'allemployees' table
                    chunk['series_id'] = chunk['series_id'].str.slice(stop=-2)

                    # Filter the chunk to remove rows where 'series_id' is in the 'production' table
                    chunk = chunk[~chunk['series_id'].isin(production['series_id'])].copy()

                    # Write the filtered chunk to the new table
                    chunk.to_sql(self.table_name, conn, if_exists='append', index=False, method='multi')

                print(f"Successfully wrote the filtered data to {self.table_name}")
            except Exception as e:
                print(f"An error occurred: {e}")





