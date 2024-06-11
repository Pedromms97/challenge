package org.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object part2 {
  def df_2(df: DataFrame): DataFrame = {

    // Cast and clean data
    val df_2 = df
      .withColumn("Rating", col("Rating").cast("double"))
      .na.fill(0)
      .filter(col("Rating") >= 4.0)
      .orderBy(col("Rating").desc)

    // Save as CSV with delimiter "§"
    val outputPath = "src/main/scala/output/best_apps"
    df_2.write
      .option("header", "true")
      .option("delimiter", "§")
      .mode("overwrite")
      .csv(outputPath)

    // Return the resulting DataFrame
    df_2
  }
}
