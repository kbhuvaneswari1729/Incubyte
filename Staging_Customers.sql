CREATE TABLE Database_name.Schema_name.Staging_Customers (
    Cust_Name VARCHAR(255) NOT NULL,
    Cust_ID VARCHAR(18) NOT NULL PRIMARY KEY,
    Open_Dt DATE NOT NULL,
    Consul_Dt DATE,
    VAC_ID CHAR(5),
    DR_Name VARCHAR(255),
    State CHAR(5),
    County CHAR(5),
    DOB DATE,
    FLAG CHAR(1),
    Age INT GENERATED ALWAYS AS (DATEDIFF(CURRENT_DATE(), DOB) / 365) STORED,
    Days_Since_Last_Consulted INT GENERATED ALWAYS AS (DATEDIFF(CURRENT_DATE(), Consul_Dt)) STORED
);
