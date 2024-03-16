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

# Loading environment variables
load_dotenv()
db_uri = os.getenv('DATABASE_URL')

# Create an instance of the DataTransformation class and call the transform_and_write_data method
data_transformation = DataTransformation(db_uri, 'women_in_government')
data_transformation.transform_and_write_data()