package com.microsoft.adf.kusto
import java.util.concurrent._
import java.io.Serializable
import com.microsoft.aad.adal4j._

class OAuthProvider(val resource: String) extends Serializable {

  val getToken = {
    val credential = new ClientCredential(Secret.getClientId, Secret.getSecret)
    val service = Executors.newFixedThreadPool(1)
    val authContext = new AuthenticationContext(Secret.getOauthEndpoint, false, service)

    val futureResult: Future[AuthenticationResult] =
      authContext.acquireToken(resource, credential, null)
    val result = futureResult.get()
    result.getAccessToken
  }
}