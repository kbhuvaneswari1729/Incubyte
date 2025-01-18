import logging
from sqlalchemy import create_engine
from etl.extract import load_data_from_staging
from etl.transform import transform_data
from etl.load import load_to_country_tables
from etl.validations import validate_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def etl_process(engine):
    """The main ETL process to load data into respective country tables."""
    logger.info("ETL process started.")

    # Step 1: Extract
    df = load_data_from_staging(engine)

    # Step 2: Validate the data
    validate_data(df)

    # Step 3: Transform the data
    df_transformed = transform_data(df)

    # Step 4: Load the data into respective country tables
    load_to_country_tables(df_transformed, engine)

    logger.info("ETL process completed.")

if __name__ == "__main__":
    engine = create_engine("postgresql://username:password@hostname:port/dbname")
    etl_process(engine)
