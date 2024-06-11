package org.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object part1 {
  def df_1(df: DataFrame): DataFrame = {

    // Read the CSV file, cast columns, and fill missing values
    val df_1 = df
      .withColumn("App", col("App").cast("string"))
      .withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("double"))
      .na.fill(0, Seq("Sentiment_Polarity"))
      .groupBy("App")
      .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))

    // Return the resulting DataFrame
    df_1
  }
}