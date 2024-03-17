from dotenv import load_dotenv
import os
from data_ingest import DataIngest
from data_structure import DataTransformation

# Loading environment variables
load_dotenv()
db_uri = os.getenv('DATABASE_URL')

try:
    # Data ingest
    ingestor = DataIngest('../data/ce.data.90a.Government.Employment.txt', db_uri)
    ingestor.read_and_write_data_in_chunks('government')

    ingestor = DataIngest('../data/ce.data.02b.AllRealEarningsAE.txt', db_uri)
    ingestor.read_and_write_data_in_chunks('allemployeesearnings')

    ingestor = DataIngest('../data/ce.data.03c.AllRealEarningsPE.txt', db_uri)
    ingestor.read_and_write_data_in_chunks('productionemployees')

    # Data transformation
    transformer = DataTransformation(db_uri, 'women_in_government')
    transformer.transform_and_write_data()

    transformer = DataTransformation(db_uri, 'production')
    transformer.filter_and_write_data(32, 'productionemployees') # 32 is the value for hourly earning that we dont want to consider

    transformer = DataTransformation(db_uri, 'allemployeesearnweek')
    transformer.filter_and_write_data(13, 'allemployeesearnings') # 13 is the value for hourly earning that we dont want to consider

    transformer = DataTransformation(db_uri, 'filtered_allemployees')
    transformer.filter_allemployees_based_on_production("production_new", "allemployeesearnweek_new")

    transformer = DataTransformation(db_uri, None)
    transformer.count_rows_and_calculate_ratio('production_new', 'filtered_allemployees_new', 'ratio_table')

except Exception as e:
    print(f"An error occurred: {e}")
