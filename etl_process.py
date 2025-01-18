import snowflake.connector
from datetime import datetime

# Establish the connection to Snowflake
def create_connection():
    return snowflake.connector.connect(
        user='<your_user>',
        password='<your_password>',
        account='<your_account>',
        warehouse='<your_warehouse>',
        database='<your_database>',
        schema='<your_schema>'
    )

# Function to calculate Age from DOB
def calculate_age(dob):
    today = datetime.today()
    birth_date = datetime.strptime(str(dob), "%Y-%m-%d")
    age = today.year - birth_date.year
    if today.month < birth_date.month or (today.month == birth_date.month and today.day < birth_date.day):
        age -= 1
    return age

# Function to calculate days since last consulted
def days_since_last_consulted(consulted_date):
    today = datetime.today()
    consult_date = datetime.strptime(str(consulted_date), "%Y-%m-%d")
    delta = today - consult_date
    return delta.days

# Function to validate the data before inserting
def validate_record(record):
    if not record['Cust_I'] or not record['Open_Dt'] or not record['DOB']:
        return False
    if record['FLAG'] not in ['A', 'I']:  # Only 'A' for Active, 'I' for Inactive
        return False
    if record['Consul_Dt'] and record['Consul_Dt'] < record['Open_Dt']:
        return False  # Last consulted date should not be before Open Date
    return True

# Function to insert data into country-specific table
def insert_data_into_country_table(cursor, record, country):
    if not validate_record(record):
        return  # Skip invalid record

    # Calculate derived columns
    age = calculate_age(record['DOB'])
    days_since_last_consulted = days_since_last_consulted(record['Consul_Dt']) if record['Consul_Dt'] else None

    # Create SQL query for inserting data
    insert_sql = f"""
        INSERT INTO Table_{country} (Cust_I, Open_Dt, Consul_Dt, VAC_ID, DR_Name, State, Country, DOB, FLAG, Age, Days_Since_Last_Consulted)
        VALUES (%(Cust_I)s, %(Open_Dt)s, %(Consul_Dt)s, %(VAC_ID)s, %(DR_Name)s, %(State)s, %(Country)s, %(DOB)s, %(FLAG)s, %(Age)s, %(Days_Since_Last_Consulted)s)
    """
    
    # Execute the SQL with the record data
    cursor.execute(insert_sql, {
        'Cust_I': record['Cust_I'],
        'Open_Dt': record['Open_Dt'],
        'Consul_Dt': record.get('Consul_Dt'),
        'VAC_ID': record.get('VAC_ID'),
        'DR_Name': record.get('DR_Name'),
        'State': record.get('State'),
        'Country': record['Country'],
        'DOB': record['DOB'],
        'FLAG': record['FLAG'],
        'Age': age,
        'Days_Since_Last_Consulted': days_since_last_consulted
    })

# Fetch data from the staging table in Snowflake
def fetch_staging_data(cursor):
    # Example query to fetch the relevant staging records
    query = """
        SELECT Cust_I, Open_Dt, Consul_Dt, VAC_ID, DR_Name, State, Country, DOB, FLAG
        FROM Staging_Customers
        WHERE FLAG = 'A';  -- Only active customers for now
    """
    cursor.execute(query)
    return cursor.fetchall()  # Return all the records fetched

# ETL process for inserting data into the country-specific tables
def etl_process(cursor):
    # Fetch all records from the Staging_Customers table
    records = fetch_staging_data(cursor)
    
    for record in records:
        # Transform row into a dictionary for easy access
        row = {
            'Cust_I': record[0],
            'Open_Dt': record[1],
            'Consul_Dt': record[2],
            'VAC_ID': record[3],
            'DR_Name': record[4],
            'State': record[5],
            'Country': record[6],
            'DOB': record[7],
            'FLAG': record[8]
        }

        # Extract the country code (Country) from the record
        country = row['Country']

        # Insert data into the corresponding country table
        insert_data_into_country_table(cursor, row, country)

# Main function to run the ETL process
def main():
    # Create Snowflake connection
    conn = create_connection()

    # Create a cursor to execute queries
    cursor = conn.cursor()

    try:
        # Start the ETL process
        etl_process(cursor)
    
    finally:
        # Close the cursor and the connection
        cursor.close()
        conn.close()

# Run the script if it's executed directly
if __name__ == "__main__":
    main()
