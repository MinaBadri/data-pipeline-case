CREATE OR REPLACE TABLE staging_accounts (
  accounts_number INT PRIMARY KEY,
  amount INT,
  account_name VARCHAR(50),
  account_type VARCHAR(50),
  reference_date DATE,
  		
);

CREATE OR REPLACE TABLE staging_loans (
    loan_id INT PRIMARY KEY,
    accounts_number INT REFERENCES staging_accounts(accounts_number),
    customer VARCHAR(50),
    customer_type VARCHAR(50),
    loan_type VARCHAR(50),
    country CHAR(10),
    amount DECIMAL(18,2),
    currency CHAR(5),
    exchange_rate DECIMAL(10,4),
    startdate DATE,
    maturity_date DATE,
    reference_date DATE,
    
);

CREATE OR REPLACE TABLE staging_deposits (
    deposit_id INT PRIMARY KEY,
    accounts_number INT REFERENCES staging_accounts(accounts_number),
    customer VARCHAR(50),
    customer_type VARCHAR(50),
    deposit_type VARCHAR(50),
    country CHAR(10),
    amount DECIMAL(18,2),
    currency CHAR(5),
    exchange_rate DECIMAL(10,4),
    startdate DATE,
    maturity_date DATE,
    reference_date DATE
);


INSERT INTO staging_accounts (accounts_number, account_type, country)
SELECT DISTINCT accounts_number, customer_type, country
FROM ext_customers
WHERE accounts_number IS NOT NULL;

INSERT INTO staging_loans (loan_id, accounts_number, customer, customer_type, loan_type, country, amount, currency, exchange_rate, startdate, maturity_date, reference_date)
SELECT DISTINCT loan_id, accounts_number, customer, customer_type, loan_type, country, amount, currency, exchange_rate, startdate, maturity_date, reference_date
FROM ext_loans
WHERE loan_id IS NOT NULL AND accounts_number IS NOT NULL;

INSERT INTO staging_deposits (deposit_id, accounts_number, customer, customer_type, deposit_type,country, amount, currency, exchange_rate, startdate, maturity_date, reference_date)
SELECT DISTINCT ddeposit_id, accounts_number, customer, customer_type, deposit_type,country, amount, currency, exchange_rate, startdate, maturity_date, reference_date
FROM ext_deposits
WHERE deposit_id IS NOT NULL AND accounts_number IS NOT NULL;


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


SELECT * FROM staging_loans WHERE amount IS NULL OR amount <= 0;
SELECT * FROM staging_deposits WHERE amount IS NULL OR amount <= 0;


SELECT s.loan_id
FROM staging_loans s
LEFT JOIN staging_customers c ON s.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

SELECT s.deposit_id
FROM staging_deposits s
LEFT JOIN staging_customers c ON s.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
