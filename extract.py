import pandas as pd
from sqlalchemy import create_engine
import logging

logger = logging.getLogger(__name__)

def load_data_from_staging(engine):
    """Load data from the staging table."""
    query = "SELECT * FROM Staging_Customers;"
    try:
        df = pd.read_sql(query, engine)
        logger.info(f"Loaded {len(df)} rows from the staging table.")
        return df
    except Exception as e:
        logger.error(f"Error loading data from staging table: {e}")
        raise
