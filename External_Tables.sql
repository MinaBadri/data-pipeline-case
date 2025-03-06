-- External Tables
-- This handles header rows, NULL values, and quoted fields
CREATE FILE FORMAT csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
NULL_IF = ('NULL', '');


CREATE OR REPLACE EXTERNAL TABLE ext_accounts
WITH LOCATION = @mystage/accounts/
FILE_FORMAT = csv_format
AUTO_REFRESH = TRUE
PATTERN = '.*accounts.*\\.csv$'
AS
SELECT $1::INT AS customer_id,
       $2::STRING AS customer_type,
       $3::STRING AS country
FROM @mystage/accounts/;


CREATE OR REPLACE EXTERNAL TABLE ext_loans
WITH LOCATION = @mystage/loans/
FILE_FORMAT = csv_format
AUTO_REFRESH = TRUE
PATTERN = '.*loans.*\\.csv$'
AS
SELECT $1::INT AS loan_id,
       $2::INT AS accounts_number,
       $3::STRING AS customer,
       $4::STRING AS customer_type,
       $5::STRING AS loan_type,
       $6::STRING AS country,
       $7::DECIMAL(18, 2) AS amount,
       $8::STRING AS currency,
       $9::DECIMAL(10, 4) AS exchange_rate,
       $10::DATE AS start_date,
       $11::DATE AS maturity_date,
       $12::DATE AS reference_date
FROM @mystage/loans/;



CREATE OR REPLACE EXTERNAL TABLE ext_deposits
WITH LOCATION = @mystage/deposits/
FILE_FORMAT = csv_format
AUTO_REFRESH = TRUE
PATTERN = '.*deposits.*\\.csv$'
AS
SELECT $1::INT AS deposit_id,
       $2::INT AS accounts_number,
       $3::VARCHAR(50) AS customer,
       $4::VARCHAR(50) AS customer_type,
       $5::VARCHAR(50) AS deposit_type, 
       $6::VARCHAR(50) AS country,
       $7::DECIMAL(18, 2) AS amount,
       $8::VARCHAR(50) AS currency,
       $9::DECIMAL(10, 4) AS exchange_rate,
       $10::DATE AS start_date,
       $11::DATE AS maturity_date,
       $12::DATE AS reference_date
FROM @mystage/deposits/;
    
       