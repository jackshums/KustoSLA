package com.microsoft.adf.kusto

/**
 * @author ${yshu}
 */
object App {
  val cluster = "https://sparkinput.azuredatalakestore.net"

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

  def main(args: Array[String]) {
    if (args.length == 0) {
      val myFt = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm.ssss")
      val t = myFt.format(System.currentTimeMillis)
      println(s"$t Hello")
    } else {
      val inputFile = args(0)
      Log.info(s"accessing input:$inputFile")
      val requests = InputParser.getRequests(inputFile)
      val inputData = requests.map(r => SLAComputeInfo(r, DataRetriever.getData(r)))
      inputData.foreach(d => {
        val database = d.input.database
        //TODO: check interval up bound
        val interval:Integer = if(d.input.interval < 5) 5 else d.input.interval
        Log.info(s"processing request for $database")
        val rows = Parser.extractRecords(d.rows)
        val fileName = TypeLib.randomString(10)
        val results = SLACompute.transformData(interval, rows).collect
        implicit val formats = org.json4s.native.Serialization.formats(org.json4s.NoTypeHints)
        val content = org.json4s.native.Serialization.write(results)
        val token = new OAuthProvider(InputParser.resource).getToken
        val jsonFile = s"${InputParser.outputPath}/$database-$fileName.json"
        Log.info(s"saving results to $jsonFile")
        AdlFile.uploadText(content, cluster, jsonFile, token)
      })
    }
  }
}
