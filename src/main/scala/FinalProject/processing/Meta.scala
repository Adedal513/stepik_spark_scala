package com.example.processing

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.time.LocalDate

class MetaWriter(
    csvFilePath: String
) {
    def gatherAndSave(spark: SparkSession, df: DataFrame, dateColumn: String, name: String): Unit = {
        val dfWithDate = df.withColumn(
            "dateColumn", // New column name for the combined date
            to_date(concat_ws("-", col("YEAR"), col("MONTH"), col("DAY")), "yyyy-M-d")
        )
        val totalCount = dfWithDate.count()
        
        val maxDate = dfWithDate.agg(max(col("dateColumn"))).head().getDate(0)
        val minDate = dfWithDate.agg(min(col("dateColumn"))).head().getDate(0)
        val currentDate = LocalDate.now().toString

        import spark.implicits._
        
        val metricsDF = Seq(
            (name, totalCount, minDate, maxDate, currentDate)
        ).toDF("name", "row_count", "collected_from", "collected_to", "processed")
        
        val fileExists = new java.io.File(csvFilePath).exists()

        if (fileExists) {
        // If the file exists, append the new row
            metricsDF.write
                .mode("append")
                .option("header", "false") // Don't write header again
                .csv(csvFilePath)
        } else {
            // If the file doesn't exist, write the header and the new row
            metricsDF.write
                .mode("overwrite")
                .option("header", "true")
                .csv(csvFilePath)
        }
    }
}