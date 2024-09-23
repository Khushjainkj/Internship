# Internship

'UpdateParquet' is a Java application using Apache Spark to compare and update records from two Parquet files based on a join condition. It outputs the updated and unmatched records to a result Parquet file. 

The application performs the following steps:
1. Reads two Parquet files.
2. Joins the datasets on specific columns.
3. Identifies and separates matched and unmatched records.
4. Updates matched records with a timestamp and flags unmatched records.
5. Writes the resulting dataset to a new Parquet file.

## Prerequisites

- Java 8 
- Apache Spark 3.0.1
- Hadoop 
- Maven (for building the project)
