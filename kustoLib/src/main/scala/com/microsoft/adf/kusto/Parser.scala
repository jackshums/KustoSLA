package com.microsoft.adf.kusto

import java.text.SimpleDateFormat
import java.sql.Timestamp
import org.apache.spark.sql.Dataset

object Parser extends java.io.Serializable {
  val isEmpty = (x: String) => x == null || Option(x.trim).forall(_.isEmpty)
  val statusToInt = (str: String) => str match { case vstr if isEmpty(vstr) => 0; case _ => Integer.parseInt(str) }

  def extractRecords(rawData: Dataset[String]): Array[KustoRecord] = {
    val dataArray = rawData.collect
    val records = dataArray.map(r => {
      implicit val formats = org.json4s.DefaultFormats
      org.json4s.jackson.JsonMethods.parse(r)
        .extract[KustoTables]
        .Tables
        .filter(t => t.TableName == "Table_0")(0).Rows
        .map(r => r match {
          case Array(x, y, z) => new KustoRecord(TypeLib.toTimestamp(x), y, TypeLib.statusToInt(z))
        })
    })
    records.flatten
  }
}