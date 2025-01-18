def load_to_country_tables(df, engine):
    """Load data into respective country tables."""
    for country in df["County"].unique():
        country_table_name = f"Table_{country.upper()}"
        df_country = df[df["County"] == country]
        df_country["Country"] = country.upper()

        try:
            df_country.to_sql(country_table_name, engine, if_exists='append', index=False)
            logger.info(f"Successfully inserted {len(df_country)} rows into {country_table_name}.")
        except Exception as e:
            logger.error(f"Error inserting data into {country_table_name}: {e}")
            raise
