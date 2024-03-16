import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

class DataIngest:
    def __init__(self, data_path, db_uri):
        self.data_path = data_path
        self.db_uri = db_uri
    
    def read_and_write_data_in_chunks(self, table_name, chunksize=10000):
        """
        Reads the data in chunks and writes each chunk to the database.
        
        Parameters:
        - table_name: The name of the database table to write to.
        - chunksize: The number of rows per chunk. Adjust based on your memory constraints.
        """
        engine = create_engine(self.db_uri)
        # Use chunksize parameter to read in chunks
        for chunk in pd.read_csv(self.data_path, delimiter="\t", chunksize=chunksize):
            chunk.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"Wrote a chunk to {table_name}")

# Load environment variables
is_loaded = load_dotenv('.env')
db_uri = os.getenv('DATABASE_URL')
print(db_uri)

# Example usage
data_path = 'ce.data.90a.Government.Employment.txt'  # Update with your actual file path
table_name = 'Government'  # Update with your actual table name

# Initialize the DataIngest object
ingestor = DataIngest(data_path, db_uri)

# Read and write data in chunks
ingestor.read_and_write_data_in_chunks(table_name)





