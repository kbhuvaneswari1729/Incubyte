from datetime import datetime

def transform_data(df):
    """Transform the data by selecting the latest record for each customer."""
    df_sorted = df.sort_values(by=["Cust_ID", "Consul_Dt"], ascending=[True, False])
    df_latest = df_sorted.drop_duplicates(subset=["Cust_ID"], keep='first')

    # Add derived columns: Age and Days_Since_Last_Consulted
    df_latest["Age"] = (datetime.now() - df_latest["DOB"]).dt.days // 365
    df_latest["Days_Since_Last_Consulted"] = (datetime.now() - df_latest["Consul_Dt"]).dt.days

    return df_latest
