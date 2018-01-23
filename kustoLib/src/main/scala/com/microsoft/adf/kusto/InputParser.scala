package com.microsoft.adf.kusto
import java.text.SimpleDateFormat 
import java.io.Serializable

class InputParser extends Serializable {
  private val inputPath = "clusters/ADFSparkTest/Input/input.json"
  private val cluster = "https://sparkinput.azuredatalakestore.net"
  //private val ft: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm")
  
  def toTimestamp = (time: String) => {
    //val time = "2017-12-27 19:35:00.0"
    //Timestamp.valueOf(time)
    new java.sql.Timestamp(TypeLib.ft.parse(time).getTime)
  }
  
  val getRequests: Array[SLASpec] = {
    val resource: String = "https://management.core.windows.net/"
    val token = new OAuthProvider(resource).getToken
    Log.info(s"accessing adl:$inputPath with $token")
    val content:String = AdlFile.readFile(cluster, inputPath, token)
    implicit val formats = org.json4s.DefaultFormats
    var json = org.json4s.jackson.JsonMethods.parse(content)
    val inputs = json.extract[Array[RawInput]]
    Log.info(s"extracted ${inputs.length} records")
    inputs.map(r => new SLASpec(r.cluster, r.database, toTimestamp(r.startTime), toTimestamp(r.endTime), r.interval)) 
  }
}