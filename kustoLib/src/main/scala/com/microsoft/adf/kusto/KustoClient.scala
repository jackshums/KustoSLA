package com.microsoft.adf.kusto

import org.apache.http._
import org.apache.http.client._
import org.apache.http.entity.StringEntity
import org.apache.http.client.methods.{ HttpPost, HttpGet }
import org.apache.http.impl.client.HttpClients

object KustoClient {

  def executeQuery(cluster: String, database: String, token: String, command: String): String = {
    val url = cluster + "/v1/rest/query"
    val client = HttpClients.createDefault()
    val post = new HttpPost(url)

    post.setHeader("Authorization", "Bearer " + token)
    post.setHeader("Content-Type", "application/json")
    post.setHeader("charset", "utf-8")
    post.setHeader("Accept-Encoding", "gzip,deflate")
    
    val query = Query(database, command, defaultProperty)
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
    val out = new java.io.StringWriter
    mapper.writeValue(out, query)
    val body = out.toString()

    val payload = new StringEntity(body, "UTF-8")
    post.setEntity(payload)
    Log.info(s"posting to $url")
    val response = client.execute(post)
    val status = response.getStatusLine().getStatusCode()
    val content = response.getEntity()
    if (content != null) {
      Log.info("parsing response entity")
      return org.apache.http.util.EntityUtils.toString(content)
    }
    return null
  }

  def defaultProperty: String = {
    "{\"Options\":{\"query_language\":\"csl\",\"queryconsistency\":\"weakconsistency\"}}"
  }
}