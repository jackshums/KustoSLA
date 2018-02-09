package com.microsoft.adf.kusto

import java.io.Serializable

object InputParser extends Serializable {
    val resource: String = "https://management.core.windows.net/"
    val outputPath = "clusters/yshuTest/output"
    private val inputPath = "clusters/ADFSparkTest/Input"


  def getRequests(jsonInputFile: String): Array[SLASpec] = {
    val token = new OAuthProvider(resource).getToken
    val inputFile = TypeLib.combine(inputPath, jsonInputFile)
    Log.info(s"accessing adl:$inputFile")
    val content: String = AdlFile.readFile(App.cluster, inputFile, token)
    implicit val formats = org.json4s.DefaultFormats
    var json = org.json4s.jackson.JsonMethods.parse(content)
    val inputs = json.extract[Array[RawInput]]
    inputs.map(r => new SLASpec(r.cluster, r.database, TypeLib.toTimestamp(r.startTime), TypeLib.toTimestamp(r.endTime), r.interval))
  }
}