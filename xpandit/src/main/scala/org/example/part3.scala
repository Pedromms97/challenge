package org.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object part3 {
  def df_3(df: DataFrame): DataFrame = {
    // Convert columns to appropriate data types and adjust Price
    val df_3 = df.withColumn("Rating", col("Rating").cast(DoubleType))
      .withColumn("Reviews", col("Reviews").cast(LongType))
      .withColumn("Price", col("Price").cast(DoubleType) * 0.9)
      // Extract numeric values from Size, handle unit conversion
      .withColumn("Size", when(col("Size").endsWith("M"), regexp_extract(col("Size"), "(\\d+\\.?\\d*)", 1).cast(DoubleType))
        .when(col("Size").endsWith("k"), regexp_extract(col("Size"), "(\\d+\\.?\\d*)", 1).cast(DoubleType) / 1000))
      // Split Genres, group by App, and aggregate columns
      .withColumn("Genres", split(col("Genres"), ";"))
      .groupBy("App")
      .agg(
        collect_set("Category").as("Categories"),
        first("Rating").alias("Rating"),
        max("Reviews").alias("Reviews"),
        first("Size").alias("Size"),
        first("Installs").alias("Installs"),
        first("Type").alias("Type"),
        first("Price").alias("Price"),
        first("Content Rating").alias("Content_Rating"),
        array_distinct(flatten(collect_list("Genres"))).alias("Genres"),
        date_format(to_date(first(col("Last Updated")), "MMMM d, yyyy"),"yyyy-MM-dd HH:mm:ss").alias("Last_Updated"),
        first("Current Ver").alias("Current_Version"),
        first("Android Ver").alias("Minimum_Android_Version")
      )
      // Fill missing values, concatenate Categories and Genres
      .na.fill(0, Seq("Rating", "Reviews", "Size"))
      .withColumn("Categories", concat_ws(",", col("Categories")))
      .withColumn("Genres", concat_ws(",", col("Genres")))

    // Return result
    df_3
  }
}
