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

def load_to_country_tables(df):
    """Load data into respective country tables in Snowflake."""
    conn = get_snowflake_connection()
    
    for country in df["County"].unique():
        country_table_name = f"TABLE_{country.upper()}"
        df_country = df[df["County"] == country]
        df_country["Country"] = country.upper()

        try:
            # Load data into Snowflake table (Use the to_sql method of pandas)
            df_country.to_sql(country_table_name, conn, if_exists='append', index=False, method='multi')
            logger.info(f"Successfully inserted {len(df_country)} rows into {country_table_name}.")
        except Exception as e:
            logger.error(f"Error inserting data into {country_table_name}: {e}")
            raise
        finally:
            conn.close()
