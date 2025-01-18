/*I am just creating only for one country, should create for all countries in the similar way by changing the name of country*/
CREATE TABLE Database_name.schema_name.Table_India (
    Cust_Name VARCHAR(255) NOT NULL,
    Cust_ID VARCHAR(18) NOT NULL PRIMARY KEY,
    Open_Dt DATE NOT NULL,
    Consul_Dt DATE,
    VAC_ID CHAR(5),
    DR_Name VARCHAR(255),
    State CHAR(5),
    Country CHAR(5),
    DOB DATE,
    FLAG CHAR(1),
    Age INT AS (DATEDIFF(CURDATE(), DOB) / 365) STORED,
    Days_Since_Last_Consulted INT AS (DATEDIFF(CURDATE(), Consul_Dt)) STORED
);
