package com.microsoft.adf.kusto
import org.apache.http._
import org.apache.http.client.methods.{ HttpPost, HttpPut, HttpGet }
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.http.entity.StringEntity

object AdlFile extends Serializable {
  def readFile (cluster: String, path: String, token: String): String = {
    val url = cluster + s"/webhdfs/v1/$path?op=OPEN"
    val client = HttpClients.createDefault()
    var request = new HttpGet(url)

    request.setHeader("Authorization", "Bearer " + token)
    request.setHeader("Content-Type", "application/json")
    request.setHeader("charset", "utf-8")
    var response = client.execute(request)
    var status = response.getStatusLine().getStatusCode()

    /*     val redirectUrl = response.getFirstHeader("Location").getValue
      val writeClient = HttpClients.createDefault()
      request = new HttpGet(redirectUrl)
      request.setHeader("Authorization", "Bearer " + token)
      request.setHeader("Content-Type", "application/json")
      request.setHeader("charset", "utf-8")

      response = writeClient.execute(request)
      status = response.getStatusLine().getStatusCode()*/
    val entity = response.getEntity
    if (status == 200 && entity != null) {
      return EntityUtils.toString(entity)
    }
    return null
  }
}