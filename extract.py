import pandas as pd
import snowflake.connector
import logging
from Incubyte.config import SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE

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

def load_data_from_staging():
    """Load data from the staging table in Snowflake."""
    query = "SELECT * FROM Staging_Customers;"
    
    conn = get_snowflake_connection()
    try:
        df = pd.read_sql(query, conn)
        logger.info(f"Loaded {len(df)} rows from the staging table.")
        return df
    except Exception as e:
        logger.error(f"Error loading data from staging table: {e}")
        raise
    finally:
        conn.close()

