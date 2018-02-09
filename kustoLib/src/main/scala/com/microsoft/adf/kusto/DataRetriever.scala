package com.microsoft.adf.kusto

import scala.annotation.tailrec
import java.sql.{ Date, Timestamp }
import org.apache.spark.sql.{ SparkSession, DataFrame, Dataset }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DataRetriever {

  def getData(input: SLASpec): Dataset[String] = {
    //contact Kusto REST API to get corresponding data
    val cluster = s"https://${input.cluster}.kusto.windows.net"
    Log.info(s"accessing $cluster ...")
    val token = new OAuthProvider(cluster).getToken
    val database = input.database
    val startTime = TypeLib.roundUpInterval(input.startTime)
    val endTime = TypeLib.roundUpInterval(input.endTime)
    val queries = getQueries(startTime, endTime, input.interval)
    import SparkLib.spark.implicits._
    val queryRdd = SparkLib.spark.createDataset(queries)
    queryRdd.map(query => {
      KustoClient.executeQuery(cluster, database, token, query)
    })
  }

  def getQueries(start: Timestamp, end: Timestamp, interval: Integer) = {
    @tailrec
    def getSubQueries(start: Timestamp, end: Timestamp, result: List[String]): Array[String] = {
      if ((start after end) || (start equals end)) {
        return result.toArray
      }
      val startTime = TypeLib.ft.format(start)
      var nextTime = TypeLib.addHour(start)
      if (nextTime after end) {
        nextTime = end
      }
      val query = s"APICalls | where TIMESTAMP > datetime($startTime) and TIMESTAMP < datetime($nextTime) | project TIMESTAMP, apiName, httpStatusCode"
      getSubQueries(nextTime, end, result :+ query)
    }
    getSubQueries(start, end, List[String]())
  }
}