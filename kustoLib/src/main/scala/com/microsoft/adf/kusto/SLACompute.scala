package com.microsoft.adf.kusto

import org.apache.spark.sql.{ SparkSession, DataFrame, Dataset, Row }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SLACompute {
  // convert failed call/total call into boolean based on 99.9%
  val slaFunc = udf((f: Long, t: Long) => t / 1000 >= f)
  // merge a number of dataset of time window slices into a single slice
  def unionFunc = (x: Dataset[SLAOutput], y: Dataset[SLAOutput]) => x union y
  // drop overlapped window
  def reduceFunc = (x: SLAOutput, y: SLAOutput) => if (!x.SLA) x else y

  val transformData: (Integer, Array[KustoRecord]) => Dataset[SLAOutput] = {
    (interval, set) =>
      {
        val intervalStr = s"$interval minutes"
        import SparkLib.spark.implicits._
        val ds = SparkLib.spark.createDataset(set)
        val wDF = ds.groupBy(window($"time", intervalStr, intervalStr))
        val slaDF = wDF.agg(count(when($"status" >= 500, true)).as("failedCallCount"))
        val tDF = wDF.agg(count("status").as("totalCalls"))
        slaDF.join(tDF, Seq("window"), "left")
          .select($"window", slaFunc($"failedCallcount", $"totalCalls") as "SLA")
          .as[SLAOutput]
      }
  }

  def writeOutput(results: Array[Dataset[SLAOutput]], outputFile: String) = {
    import SparkLib.spark.implicits._
    val outputData = results.reduce(unionFunc)
      .groupByKey(r => r.window)
      .reduceGroups(reduceFunc)
      .map { case (x, y) => y }
    outputData.write.format("json").save(outputFile)
  }
}