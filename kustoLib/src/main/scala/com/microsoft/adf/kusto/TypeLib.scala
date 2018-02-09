package com.microsoft.adf.kusto

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.util.{ Try, Success, Failure }
import org.apache.spark.sql.Dataset

case class KustoColumnType(ColumnName: String, ColumnType: String, DataType: String)
case class KustoTable(Columns: Array[KustoColumnType], Rows: Array[Array[String]], TableName: String)
case class KustoTables(Tables: Array[KustoTable])
case class KustoRecord(time: java.sql.Timestamp, api: String, status: Integer) extends java.io.Serializable
case class Query(db: String, csl: String, properties: String)
case class RawInput(cluster: String, database: String, startTime: String, endTime: String, interval: Integer)
case class SLASpec(cluster: String, database: String, startTime: java.sql.Timestamp, endTime: java.sql.Timestamp, interval: Integer)
case class TWindow(start: Timestamp, end: Timestamp)
case class SLAOutput(window:TWindow, SLA:Boolean)
case class SLAComputeInfo(input:SLASpec, rows: Dataset[String])

object TypeLib extends App {

  def ft: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm")
  def isEmpty(x: String) = x == null || Option(x.trim).forall(_.isEmpty)

  def statusToInt(str: String): Integer = str match { case vstr if isEmpty(vstr) => 0; case _ => Integer.parseInt(str) }

  def toTimestamp = (time: String) => {
    //val time = "2017-12-27 19:35:00"
    //Timestamp.valueOf(time)
    new Timestamp(ft.parse(time).getTime)
  }

  def getTimestamp(s: String): Option[Timestamp] = s match {
    case "" => None
    case _ => {
      Try(new Timestamp(ft.parse(s).getTime)) match {
        case Success(t) => Some(t)
        case Failure(_) => None
      }
    }
  }

  // the boundary time has to be aligned to 5 minutes multiply
  def roundUpInterval(input: Timestamp): Timestamp = {
    val calendar = Calendar.getInstance()
    calendar.setTime(input)
    val minutes = calendar.get(Calendar.MINUTE)
    var roundUp = minutes
    if (minutes % 5 != 0) {
      roundUp = (minutes / 5 + 1) * 5
    }
    calendar.set(Calendar.MINUTE, roundUp)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    new Timestamp(calendar.getTime.getTime)
  }

  def roundUpTimeString(input: String): String = {
    val inputTime = TypeLib.ft.parse(input)
    val resultTime = roundUpInterval(new Timestamp(inputTime.getTime))
    TypeLib.ft.format(resultTime)
  }

  def randomString(length: Int) = {
    val r = scala.util.Random.alphanumeric
    val sb = new StringBuilder
    r take length foreach sb.append
    sb.toString
  }

  def combine(root: String, file: String): String = {
    java.nio.file.Paths.get(root, file).toString
  }

  def addHour(input: Timestamp): Timestamp = {
    val cal = Calendar.getInstance()
    cal.setTime(input)
    val hours = cal.get(Calendar.HOUR)
    cal.set(Calendar.HOUR, hours + 1)
    new Timestamp(cal.getTime.getTime)
  }

  def sampleJson: String = """{
	"Tables": [{
		"TableName": "Table_0",
		"Columns": [{
			"ColumnName": "TIMESTAMP",
			"DataType": "DateTime",
			"ColumnType": "datetime"
		}, {
			"ColumnName": "apiName",
			"DataType": "String",
			"ColumnType": "string"
		}, {
			"ColumnName": "httpStatusCode",
			"DataType": "Int64",
			"ColumnType": "long"
		}],
		"Rows": [
			["2017-12-08T23:47:02.83198Z", "GET dataFactories\\", 404],
			["2017-12-08T23:46:22.3749703Z", "GET dataFactories\\", 404],
			["2017-12-09T00:10:46.2418346Z", "GET dataFactories\\", 404]
		]
	}, {
		"TableName": "Table_1",
		"Columns": [{
			"ColumnName": "Value",
			"DataType": "String",
			"ColumnType": "string"
		}],
		"Rows": [
			["{\"Visualization\":\"table\",\"Title\":\"\",\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":\"\",\"Annotation\":\"\",\"By\":null}"]
		]
	}, {
		"TableName": "Table_2",
		"Columns": [{
			"ColumnName": "Timestamp",
			"DataType": "DateTime",
			"ColumnType": "datetime"
		}, {
			"ColumnName": "Severity",
			"DataType": "Int32",
			"ColumnType": "int"
		}, {
			"ColumnName": "SeverityName",
			"DataType": "String",
			"ColumnType": "string"
		}, {
			"ColumnName": "StatusCode",
			"DataType": "Int32",
			"ColumnType": "int"
		}, {
			"ColumnName": "StatusDescription",
			"DataType": "String",
			"ColumnType": "string"
		}, {
			"ColumnName": "Count",
			"DataType": "Int32",
			"ColumnType": "int"
		}, {
			"ColumnName": "RequestId",
			"DataType": "Guid",
			"ColumnType": "guid"
		}, {
			"ColumnName": "ActivityId",
			"DataType": "Guid",
			"ColumnType": "guid"
		}, {
			"ColumnName": "SubActivityId",
			"DataType": "Guid",
			"ColumnType": "guid"
		}, {
			"ColumnName": "ClientActivityId",
			"DataType": "String",
			"ColumnType": "string"
		}],
		"Rows": [
			["2017-12-09T00:31:53.4036329Z", 4, "Info", 0, "Query completed successfully", 1, "faa96799-a330-4cb1-8bdf-c77ed5723e80", "faa96799-a330-4cb1-8bdf-c77ed5723e80", "eca475a9-a2a2-4bc3-bc23-7976e4b717fc", "unspecified;1386ce20-3313-4089-a973-44e61d967b00"],
			["2017-12-09T00:31:53.4036329Z", 6, "Stats", 0, "{\"ExecutionTime\":3.1718606,\"resource_usage\":{\"cache\":{\"memory\":{\"hits\":101,\"misses\":5,\"total\":106},\"disk\":{\"hits\":5,\"misses\":0,\"total\":5}},\"cpu\":{\"user\":\"00:00:00\",\"kernel\":\"00:00:00\",\"total cpu\":\"00:00:00\"},\"memory\":{\"peak_per_node\":144876960}},\"dataset_statistics\":[{\"table_row_count\":94,\"table_size\":3136}]}", 1, "faa96799-a330-4cb1-8bdf-c77ed5723e80", "faa96799-a330-4cb1-8bdf-c77ed5723e80", "eca475a9-a2a2-4bc3-bc23-7976e4b717fc", "unspecified;1386ce20-3313-4089-a973-44e61d967b00"]
		]
	}, {
		"TableName": "Table_3",
		"Columns": [{
			"ColumnName": "Ordinal",
			"DataType": "Int64",
			"ColumnType": "long"
		}, {
			"ColumnName": "Kind",
			"DataType": "String",
			"ColumnType": "string"
		}, {
			"ColumnName": "Name",
			"DataType": "String",
			"ColumnType": "string"
		}, {
			"ColumnName": "Id",
			"DataType": "String",
			"ColumnType": "string"
		}, {
			"ColumnName": "PrettyName",
			"DataType": "String",
			"ColumnType": "string"
		}],
		"Rows": [
			[0, "QueryResult", "PrimaryResult", "f4d43668-025a-4935-983f-32c317524630", null],
			[1, "", "@ExtendedProperties", "cc67acff-6873-4e76-93f4-3e40d7f7c109", null],
			[2, "QueryStatus", "QueryStatus", "00000000-0000-0000-0000-000000000000", null]
		]
	}]
}""".stripMargin
}