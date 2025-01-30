package com.example.utils

import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {
  def buildSession(appName: String, master: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master(master)
      .config("spark.sql.shuffle.partitions", "200")
      .getOrCreate()
  }
}
