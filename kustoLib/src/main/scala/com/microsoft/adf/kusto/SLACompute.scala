package com.microsoft.adf.kusto

import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SLACompute {
  val slaFunc = udf((f: Long, t: Long) => t / 1000 >= f)

  val getResults = (spark: SparkSession, set: Array[Array[String]]) => {
    val typedSet = set.map(r => r match {
      case Array(x, y, z) => new KustoRecord(TypeLib.toTimestamp(x), y, TypeLib.statusToInt(z))
    })
    import spark.implicits._
    val ds = spark.createDataset(typedSet)
    val wDF = ds.groupBy(window($"time", "15 minutes", "15 minutes"))
    val slaDF = wDF.agg(count(when($"status" >= 500, true)).as("failedCallCount"))
    val tDF = wDF.agg(count("status").as("totalCalls"))
    val joined = slaDF.join(tDF, Seq("window"), "left")
    val results = joined.select($"window", slaFunc($"failedCallcount", $"totalCalls") as "SLA").sort($"window".asc)
    val rnd = TypeLib.randomString(10)
    val logFile = s"adl://sparkinput.azuredatalakestore.net/clusters/yshuTest/output/SLAout$rnd"
    results.show
    results.write.mode("overwrite").json(logFile)
  }
}