# Data Pipeline Case

## Overview

This repository outlines the steps to migrate data management from Excel to Snowflake.

To automate the process of loading data from a local system to S3, **Apache Airflow** is used. You can refer to the `Load_to_s3.py` file for the implementation.

To set up the pipeline to load data from S3 to Snowflake's external storage, follow the official guide: [Snowflake External Storage Setup](https://docs.snowflake.com/en/user-guide/tables-external-s3).  
Since there was no conflicting event notification for my S3, I used **Option 1: Creating a new S3 event notification** to automate the refreshing of external table metadata using **Amazon SQS**.  
The two SQL files in the repository are used to create external tables and staging tables before we move the data to final tables in Snowflake.

## Data Modeling Proposal

The proposed data model will be based on a **Star Schema**, ideal for reporting and analytics. The model will consist of a central fact table and several dimension tables, organizing data in a way that is intuitive for business stakeholders while remaining flexible for technical purposes. Below is the schema breakdown:

### Fact Table: `fact_transactions`

This table will store transaction-related data, including loans, deposits, and account activities. It will contain metrics such as `amount`, `currency`, `start_date`, `maturity_date`, and `reference_date`, along with foreign keys referencing the dimension tables.

### Dimension Tables:

- **`Dim_Customer`**: Stores unique customer information (`customer_id`, `customer_type`, `country`).
- **`Dim_Account`**: Contains information about accounts (`account_id`, `account_type`, `account_name`).
- **`Dim_Loan`**: Contains details about loans (`loan_id`, `loan_type`).
- **`Dim_Deposit`**: Contains details about deposits (`deposit_id`, `deposit_type`).
- **`Dim_Currency`**: Stores currency information (`currency_id`, `exchange_rate`).

### Flexibility

The model is designed to be scalable, allowing for future expansion. New dimensions can be added as the business introduces new types of products or reports without disrupting existing queries.

### Business Meaning

The schema and relationships between tables are designed to be easily understood by non-technical stakeholders, such as business users and upper management. For example, `fact_transactions` connects with `Dim_Customer`, `Dim_Loan`, `Dim_Account`, `Dim_Deposit`, and `Dim_Currency` to enable both cross-sectional and time-based reporting.

![Image Description](https://github.com/MinaBadri/data-pipeline-case/blob/main/DataModel.png)
