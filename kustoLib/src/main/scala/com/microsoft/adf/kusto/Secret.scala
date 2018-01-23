package com.microsoft.adf.kusto

object Secret {
  private val clientId: String = "xxx"
  private val clientSecret: String = "***"
  private val oauthEndpoint: String = "https://login.microsoftonline.com/microsoft.com/"

  def getClientId:String = clientId
  def getSecret:String = clientSecret
  def getOauthEndpoint:String = oauthEndpoint
  def getResource(cluster:String) : String = s"https://$cluster.kusto.windows.net"
}