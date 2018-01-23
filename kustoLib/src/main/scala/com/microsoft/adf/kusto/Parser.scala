package com.microsoft.adf.kusto

import java.text.SimpleDateFormat
import java.sql.Timestamp

class Parser(jsonStr: String) extends java.io.Serializable {
  val isEmpty = (x: String) => x == null || Option(x.trim).forall(_.isEmpty)
  val statusToInt = (str: String) => str match { case vstr if isEmpty(vstr) => 0; case _ => Integer.parseInt(str) }
  
  def extractRecords:Array[Array[String]] = {
    implicit val formats = org.json4s.DefaultFormats
    var parsed = org.json4s.jackson.JsonMethods.parse(jsonStr)
    val results = parsed.extract[KustoTables]
    results.Tables.filter(t => t.TableName == "Table_0")(0).Rows
  }
}