package org.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object part5 {
  // no enunciado diz para usar o df_3, mas nesse não está contida a coluna average sentiment polarity, por isso usei o df criado na parte 4 df1_3
  def df_4(df1_3: DataFrame): DataFrame = {

    // Explode Genres into separate rows
    val dfExploded = df1_3.withColumn("Genre", explode(split(col("Genres"), ",")))

    // Group by Genre and calculate the number of applications, average rating, and average sentiment polarity
    val df_4 = dfExploded.groupBy("Genre")
      .agg(
        count("App").alias("Count"),
        avg("Rating").alias("Average_Rating"),
        avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
      )

    // Save the final DataFrame as a parquet file with gzip compression
    val outputPath = "src/main/scala/output/googleplaystore_metrics"
    df_4.write
      .option("compression", "gzip")
      .mode("overwrite")
      .parquet(outputPath)

    // Return the resulting DataFrame
    df_4
  }
}
