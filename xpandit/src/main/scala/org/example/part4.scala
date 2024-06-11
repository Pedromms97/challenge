package org.example

import org.apache.spark.sql.DataFrame


object part4 {
  def df1_3(df1: DataFrame, df3: DataFrame): DataFrame = {
    // Join df1 and df3 on 'App'
    val finalDF = df3.join(df1, Seq("App"), "inner")

    // Define the output path
    val outputPath = "src/main/scala/output/googleplaystore_cleaned"

    // Save the final DataFrame as a parquet file with gzip compression
    finalDF.write
      .option("compression", "gzip")
      .mode("overwrite")
      .parquet(outputPath)

    // Return the resulting DataFrame
    finalDF
  }
}