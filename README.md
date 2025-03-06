# data-pipeline-case
Data engineering case
To set-up the pipeline to load data from S3 to Snowflake's External storage, I used Snowflake documentation. You can follow the steps here: https://docs.snowflake.com/en/user-guide/tables-external-s3
Since I didn't have a conflicting event notification for my S3, for automating the refreshing of external table metadata using Amazon SQS I used "Option 1: Creating a new S3 event notification"
