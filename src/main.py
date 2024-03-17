from dotenv import load_dotenv
import os
from data_ingest import DataIngest
from data_structure import DataTransformation

# Loading environment variables
load_dotenv()
db_uri = os.getenv('DATABASE_URL')

#Data ingest

# Women in government
#data_path = '../data/ce.data.90a.Government.Employment.txt' 
#table_name = 'government'  

# Initialize the DataIngest object with your data path and database URI
# ingestor = DataIngest(data_path, db_uri)

# Call the correct method to read the data from the CSV file and write it to the database
# ingestor.read_and_write_data_in_chunks(table_name)

# All employees earnings
# data_path = '../data/ce.data.02b.AllRealEarningsAE.txt' 
# table_name = 'allemployeesearnings'  

# Initialize the DataIngest object with your data path and database URI
# ingestor = DataIngest(data_path, db_uri)

# Call the correct method to read the data from the CSV file and write it to the database
# ingestor.read_and_write_data_in_chunks(table_name)


# Production and non-supervisory employees
# data_path = '../data/ce.data.03c.AllRealEarningsPE.txt'
# table_name = 'productionemployees'

# Initialize the DataIngest object with your data path and database URI
# ingestor = DataIngest(data_path, db_uri)

# Call the correct method to read the data from the CSV file and write it to the database
# ingestor.read_and_write_data_in_chunks(table_name)


# Data transformation

# data_transformation = DataTransformation(db_uri, 'women_in_government')
# data_transformation.transform_and_write_data()


# Production records transformation 

# Create an instance of the DataTransformation class
# data_transformation = DataTransformation(db_uri, 'production')

# Call the transform_and_write_data_production method
# data_transformation.filter_and_write_data_production(32, 'productionemployees')


# Filter out average hourly earnings for all employees

# Create an instance of the DataTransformation class
# data_transformation = DataTransformation(db_uri, 'allemployeesearnweek')

# Call the transform_and_write_data_production method
# data_transformation.filter_and_write_data_production(13, 'allemployeesearnings')


# Filter all employees based on production records
# Create an instance of the DataTransformation class
data_transformation = DataTransformation(db_uri, 'filtered_allemployees')

# Call the filter_allemployees_based_on_production method
data_transformation.filter_allemployees_based_on_production("production_new", "allemployeesearnweek_new")