package com.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;

public class UpdateParquet {
    public static void main(String[] args) {
        SparkConf sparkconf = new SparkConf().setAppName("UpdateParquet").setMaster("local")
                .set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
                .set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        SparkSession spark = SparkSession
                .builder()
                .config(sparkconf)
                .getOrCreate();

        String parquetFilePath1 = "";
        String parquetFilePath2 = "";
        String resultFilePath = "";

        try {
            System.out.println("Start");

            Dataset<Row> parquetData1 = spark.read().parquet(parquetFilePath1);
            System.out.println("parquetData1:");
            parquetData1.show(5);

            Dataset<Row> parquetData2 = spark.read().parquet(parquetFilePath2);
            System.out.println("parquetData2:");
            parquetData2.show(5);



            Column joinCondition = parquetData1.col("account").equalTo(parquetData2.col("account"))
                    .and(parquetData1.col("source_id").equalTo(parquetData2.col("source_id")))
                    .and(parquetData1.col("destination_id").equalTo(parquetData2.col("destination_id")))
                    .and(parquetData1.col("operator_id").equalTo(parquetData2.col("operator_id")))
                    .and(parquetData1.col("new_dynamic_commision_rate").equalTo(parquetData2.col("new_dynamic_commision_rate")));

            Dataset<Row> matchedRecords = parquetData1.join(parquetData2, joinCondition, "inner");
//            System.out.println("matchedRecords:");
//            matchedRecords.show(5);

            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime now = LocalDateTime.now();
            String currentTime = now.format(dtf);

            Dataset<Row> updatedRecords = matchedRecords.select(
                    parquetData1.col("account"),
                    parquetData1.col("source_id"),
                    parquetData1.col("destination_id"),
                    parquetData1.col("operator_id"),
                    parquetData1.col("new_dynamic_commision_rate").alias("previous_dynamic_commision_rate"),
                    parquetData2.col("new_dynamic_commision_rate").alias("new_dynamic_commision_rate"),
                    functions.lit(true).alias("is_active"),
                    functions.lit(currentTime).cast(DataTypes.TimestampType).alias("created_DateTime"),
                    functions.lit(currentTime).cast(DataTypes.TimestampType).alias("last_updated_datetime")
            );
//            System.out.println("updatedRecords:");
//            updatedRecords.show(5);


            Dataset<Row> unmatchedRecords1 = parquetData1.join(parquetData2, joinCondition, "left_anti")
                    .select(
                            parquetData1.col("account"),
                            parquetData1.col("source_id"),
                            parquetData1.col("destination_id"),
                            parquetData1.col("operator_id"),
                            parquetData1.col("new_dynamic_commision_rate"),
                            functions.lit(null).cast(DataTypes.StringType).alias("new_dynamic_commision_rate"),
                            functions.lit(false).alias("is_active"),
                            functions.lit(null).cast(DataTypes.TimestampType).alias("created_DateTime"),
                            functions.lit(null).cast(DataTypes.TimestampType).alias("last_updated_datetime")
                    );
//            System.out.println("unmatchedRecords1:");
//            unmatchedRecords1.show(5);

            Dataset<Row> unmatchedRecords2 = parquetData2.join(parquetData1, joinCondition, "left_anti")
                    .select(
                            parquetData2.col("account"),
                            parquetData2.col("source_id"),
                            parquetData2.col("destination_id"),
                            parquetData2.col("operator_id"),
                            functions.lit(null).cast(DataTypes.StringType).alias("previous_dynamic_commision_rate"),
                            parquetData2.col("new_dynamic_commision_rate").alias("new_dynamic_commision_rate"),
                            functions.lit(false).alias("is_active"),
                            functions.lit(null).cast(DataTypes.TimestampType).alias("created_DateTime"),
                            functions.lit(null).cast(DataTypes.TimestampType).alias("last_updated_datetime")
                    );
//            System.out.println("unmatchedRecords2:");
//            unmatchedRecords2.show(5);

            Dataset<Row> unmatchedRecords = unmatchedRecords1.union(unmatchedRecords2);
//            System.out.println("unmatchedRecords:");
//            unmatchedRecords.show(5);

            Dataset<Row> combinedData = updatedRecords.union(unmatchedRecords);
            System.out.println("combinedData:");
            combinedData.show(5);

            if (combinedData.isEmpty()) {
                System.err.println("combinedData is empty. Exiting.");
                return;
            }


            combinedData.show(10);
            combinedData.write().parquet(resultFilePath);

            System.out.println("Data written to: " + resultFilePath);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error reading or writing parquet file: " + e.getMessage());
        } finally {
            spark.stop();
        }
    }
}
