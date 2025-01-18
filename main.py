import logging
from etl_process.extract import load_data_from_staging
from etl_process.transform import transform_data
from etl_process.load import load_to_country_tables
from etl_process.validations import validate_data
from etl_process.config import SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE
import snowflake.connector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_snowflake_connection():
    """Create and return a Snowflake connection."""
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    return conn

def etl_processing():
    """The main ETL process to load data into respective country tables."""
    logger.info("ETL process started.")

    # Step 1: Extract
    conn = get_snowflake_connection()
    df = load_data_from_staging()

    # Step 2: Validate the data
    validate_data(df)

    # Step 3: Transform the data
    df_transformed = transform_data(df)

    # Step 4: Load the data into respective country tables
    load_to_country_tables(df_transformed)

    logger.info("ETL process completed.")

if __name__ == "__main__":
    try:
        etl_processing()
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
