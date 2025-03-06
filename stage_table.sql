CREATE OR REPLACE TABLE staging_customers AS
SELECT DISTINCT
    customer VARCHAR(50) PRIMARY KEY ,
    customer_type VARCHAR(50),
    country CHAR(10)
FROM (
    SELECT customer,  customer_type, country FROM ext_deposits
    UNION ALL
    SELECT customer,  customer_type, country  FROM ext_loans
)
WHERE customer IS NOT NULL;

CREATE OR REPLACE TABLE staging_currency AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY currency) AS PRIMARY_KEY,
    currency CHAR(10),
    exchange_rate DECIMAL(10,2)
FROM (
    SELECT currency, exchange_rate FROM ext_loans
    UNION ALL
    SELECT currency, exchange_rateFROM ext_deposits
)
WHERE currency IS NOT NULL;


CREATE OR REPLACE TABLE staging_accounts AS
SELECT DISTINCT
    accounts_number INT PRIMARY KEY,
    account_name VARCHAR(50),
    account_type VARCHAR(50),
    reference_date DATE
FROM (
    SELECT accounts_number, account_name, account_type, reference_date FROM ext_accounts
)
WHERE accounts_number IS NOT NULL;



CREATE OR REPLACE TABLE staging_loans AS
SELECT DISTINCT
    loan_id VARCHAR(50) PRIMARY KEY ,
    loan_type VARCHAR(50)
FROM ext_loans
WHERE loan_id IS NOT NULL;


CREATE OR REPLACE TABLE staging_deposits AS
SELECT DISTINCT
    deposit_id VARCHAR(50) PRIMARY KEY ,
    deposit_type VARCHAR(50)
FROM ext_deposits
WHERE deposit_id IS NOT NULL;



CREATE OR REPLACE TABLE staging_facts AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY reference_date) AS PRIMARY_KEY,
    NULL AS loan_id, 
    NULL AS accounts_number, 
    customer,
    NULL AS deposit_id, 
    amount,
    currency,
    startdate,
    maturity_date,
    reference_date
FROM (
    SELECT loan_id, 
           NULL AS accounts_number, 
           customer, 
           NULL AS deposit_id, 
           amount, 
           currency, 
           startdate, 
           maturity_date, 
           reference_date
    FROM ext_loans

    UNION ALL

    SELECT NULL AS loan_id, 
           accounts_number, 
           NULL AS customer, 
           NULL AS deposit_id, 
           amount, 
           NULL AS currency, 
           NULL AS startdate, 
           NULL AS maturity_date, 
           reference_date
    FROM ext_accounts

    UNION ALL

    SELECT NULL AS loan_id, 
           NULL AS accounts_number, 
           customer, 
           deposit_id, 
           amount, 
           currency, 
           startdate, 
           maturity_date, 
           reference_date
    FROM ext_deposits
);




CREATE OR REPLACE TABLE staging_loans_dedup AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY start_date DESC) AS row_num
    FROM staging_loans


) WHERE row_num = 1;

CREATE OR REPLACE TABLE staging_deposits_dedup AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY deposit_id ORDER BY start_date DESC) AS row_num
    FROM staging_deposits

) WHERE row_num = 1;

CREATE OR REPLACE TABLE staging_accounts_dedup AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY accounts_number ORDER BY start_date DESC) AS row_num
    FROM staging_accounts

) WHERE row_num = 1;


CREATE OR REPLACE TABLE staging_customers_dedup AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer ORDER BY start_date DESC) AS row_num
    FROM staging_customers

) WHERE row_num = 1;    

CREATE OR REPLACE TABLE staging_facts_dedup AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY reference_date ORDER BY start_date DESC) AS row_num
    FROM staging_facts

) WHERE row_num = 1;


-- SELECT * FROM staging_loans WHERE amount IS NULL OR amount <= 0;
-- SELECT * FROM staging_deposits WHERE amount IS NULL OR amount <= 0;


-- SELECT s.loan_id
-- FROM staging_loans s
-- LEFT JOIN staging_customers c ON s.customer_id = c.customer_id
-- WHERE c.customer_id IS NULL;

-- SELECT s.deposit_id
-- FROM staging_deposits s
-- LEFT JOIN staging_customers c ON s.customer_id = c.customer_id
-- WHERE c.customer_id IS NULL;
