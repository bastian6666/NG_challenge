import pandas as pd
from sqlalchemy import create_engine

class DataIngest:
    def __init__(self, data_path, db_uri):
        self.data_path = data_path
        self.db_uri = db_uri
    
    def read_and_write_data_in_chunks(self, table_name, chunksize=10000):
        """
        Reads the data in chunks, strips spaces from column names and string values,
        and writes each chunk to the database.
        
        Parameters:
        - table_name: The name of the database table to write to.
        - chunksize: The number of rows per chunk. Adjust based on memory constraints.
        """
        engine = create_engine(self.db_uri)
        for chunk in pd.read_csv(self.data_path, delimiter="\t", chunksize=chunksize):
            # Correct the column names: strip spaces and ensure lowercase for consistency
            chunk.columns = [col.strip().lower() for col in chunk.columns]
            # Optionally, strip spaces from string values in each column
            for col in chunk.select_dtypes(include=['object']).columns:
                chunk[col] = chunk[col].str.strip()
            
            # Write the cleaned chunk to the database
            try:
                chunk.to_sql(table_name, engine, if_exists='append', index=False, method='multi')
                print(f"Successfully wrote a chunk to {table_name}")
            except Exception as e:
                print(f"An error occurred: {e}")
                break  # Stop the loop if an error occurs











