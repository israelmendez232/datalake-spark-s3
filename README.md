# DataLake with Spark and S3+EMR
##### [Israel Mendes](israelmendes.com.br)
Creating a data lake with Amazon S3 and EMR and Spark, running with PySpark.

--- 

## Process
Step By Step:
1. Fill the `dl.cfg` file with configuration data from the Data Lake in S3 of my account. For security reasons, the file was not included/or empty;
2. Run the `etl.py` script, to create and organize the following:
 - Select the sources and create the tables in PySpark;
 - Read the data from the sources and insert in the tables.
 - Create an output file to present the cleaned data.
 
 ## Command
The only command needed is:
> python etl.py
Remember to create and update `dl.cfg`.
