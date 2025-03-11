# Spotify_spark_etl
Spark ETL Pipeline to extract Spotify data daily and create a DB using AWS and Snowflake.

Tools:
1. Spotify Developer Dashboard: To get the Spotify API client_id and client_secret(this is required to extract data from Spotify website) [Link](https://developer.spotify.com/dashboard)
2. AWS(S3, Glue, Lambda, CloudWatch, IAM) : For Extract, Transform and Load the data.
3. Snowflake: To maintain database.

Steps:
1. **Extract**: Get the data using Spotipy library in AWS Lambda. This data will be in json format and will be extracted daily(using CloudWatch trigger).
2. Save this extracted data to a S3 location.
3. **Transform**: This saved data will be transformed using AWS Glue. Create a daily schedule. This transformation will give us the data in the required structured format(or tables).
Note: We use pySpark in Glue for Extract, Transform and Load. 
4. Save this transformed data into another S3 location(this is final S3 location).
5. **Load**: In Snowflake, a stage will be created to load the data from S3 final location.
6. Snowpipe will be created to automate the loading of data into snowflake table so that whenever new file is added to S3 final location, it will get loaded in the snowflake tables too.
   
