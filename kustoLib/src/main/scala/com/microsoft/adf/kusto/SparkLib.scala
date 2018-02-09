package com.microsoft.adf.kusto
import org.apache.spark.sql.{ SparkSession, DataFrame }

object SparkLib {
  @transient lazy val spark = SparkSession.builder().getOrCreate()
}