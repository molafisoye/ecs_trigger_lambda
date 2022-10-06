package com.gex

import com.gex.EcsTaskHandler.Response
import com.google.gson.Gson
import java.io._
import java.util.ArrayList
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicNameValuePair

object HttpHandler  {
    def sendTaskNotification(notification: Notification): Response = {
        val notificationJson = new Gson().toJson(notification)
        val post = new HttpPost("https://q2dnwvre6cd26dalwrsbuawnry0bahla.lambda-url.us-east-1.on.aws/")
        val nameValuePairs = new ArrayList[NameValuePair]()
        nameValuePairs.add(new BasicNameValuePair("JSON", notificationJson))
        post.setEntity(new UrlEncodedFormEntity(nameValuePairs))
        val client = HttpClientBuilder.create().build()
        val response = client.execute(post)

        Response(response.getStatusLine.getReasonPhrase, response.getStatusLine.getStatusCode)
    }

case class Notification(jobName: String, status: String)
}
