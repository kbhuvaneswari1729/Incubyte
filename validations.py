import pandas as pd
import logging

logger = logging.getLogger(__name__)

def validate_data(df):
    """Perform validations on the data."""
    logger.info("Validating data...")

    # Ensure mandatory fields are not null
    mandatory_fields = ["Cust_Name", "Cust_ID", "Open_Dt"]
    for field in mandatory_fields:
        if df[field].isnull().any():
            logger.error(f"Mandatory field '{field}' has null values.")
            raise ValueError(f"Mandatory field '{field}' has null values.")

    # Ensure date formats are correct (YYYYMMDD)
    date_columns = ["Open_Dt", "Consul_Dt", "DOB"]
    for col in date_columns:
        try:
            df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='raise')
        except Exception as e:
            logger.error(f"Invalid date format in column {col}: {e}")
            raise ValueError(f"Invalid date format in column {col}: {e}")

    # Validate flag values (only 'A' or 'I')
    if not df["FLAG"].isin(["A", "I"]).all():
        logger.error("Invalid FLAG values detected. Only 'A' or 'I' are allowed.")
        raise ValueError("Invalid FLAG values detected. Only 'A' or 'I' are allowed.")

    logger.info("Data validation completed successfully.")
