package com.microsoft.adf.kusto

/**
 * @author ${yshu}
 */
object App {

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

  def main(args: Array[String]) {
    println("Hello World!")
    val jsonStr = TypeLib.sampleJson
    val records = new Parser(jsonStr).extractRecords
    println(s"demo extracted ${records.length} rows")
  }

}
