package org.example

import org.apache.spark.sql.SparkSession
import org.example.part1.df_1
import org.example.part2.df_2
import org.example.part3.df_3
import org.example.part4.df1_3
import org.example.part5.df_4

object main {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("xpandit")
      .master("local[*]")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()

    // Define the file path for the CSV files, Read  and create a DataFrame
    val csvPath_googleplaystore_user_reviews = "src/main/scala/csv_files/googleplaystore_user_reviews.csv"
    val df_googleplaystore_user_reviews = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .option("quote", "\"")
      .csv(csvPath_googleplaystore_user_reviews)

    val csvpathgoogleplaystore = "src/main/scala/csv_files/googleplaystore.csv"
    val df_googleplaystore = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .option("quote", "\"")
      .csv(csvpathgoogleplaystore)

    // Apply transformations from dataframes
    val part1 = df_1(df_googleplaystore_user_reviews)
    val part2 = df_2(df_googleplaystore)
    val part3 = df_3(df_googleplaystore)
    val part4 = df1_3(part1, part3)
    val part5 = df_4(part4)

    // Show the resulting DataFrame
    part1.show(10, false)
    part2.show(10, false)
    part3.show(10, false)
    part4.show(10, false)
    part5.show(10, false)

    // Stop SparkSession
    spark.stop()
  }
}
