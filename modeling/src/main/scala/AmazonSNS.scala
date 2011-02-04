package edu.berkeley.cs
package scads
package piql
package modeling

import deploylib.mesos._
import comm._
import storage._
import piql._
import perf._
import avro.marker._
import avro.runtime._

import net.lag.logging.Logger
import java.io.File
import java.net._

import scala.collection.JavaConversions._
import scala.collection.mutable._

import com.amazonaws.services.sns._
import com.amazonaws.auth._
import com.amazonaws.services.sns.model._

class SimpleAmazonSNSClient {
  val snsClient = createAmazonSNSClient
  
  def createAmazonSNSClient:AmazonSNSClient = {
    return new AmazonSNSClient(new BasicAWSCredentials("AKIAILGVHXBVDZKFJZQQ", "VdB4xNttSvG8DOeF90XQI4jqg6EOi6L00nt0Lq3n"))
  }
  
  // ARN = Amazon Resource Name
  def createTopicAndReturnTopicArn(topicText: String):String = {
    val res = snsClient.createTopic(new CreateTopicRequest(topicText))
    res.getTopicArn
  }
  
  def createOrRetrieveTopicAndReturnTopicArn(topicText: String):String = {
    val topics:Buffer[Topic] = snsClient.listTopics.getTopics
    val topicsList:List[Topic] = topics.toList
    val matchingTopics:List[Topic] = topicsList.filter(t => t.getTopicArn.contains(topicText))
    val topicArn = matchingTopics.length match {
      case 0 => createTopicAndReturnTopicArn(topicText)
      case 1 => matchingTopics.head.getTopicArn
      case 2 => 
        throw new TopicException
        matchingTopics.head.getTopicArn
    }
    topicArn
  }
  
  def subscribeViaEmail(topicArn: String, emailAddress: String) = {
    snsClient.subscribe(new SubscribeRequest(topicArn, "email", emailAddress))
  }
  
  def publishToTopic(topicArn: String, message: String, subject: String) = {
    snsClient.publish(new PublishRequest(topicArn, message, subject))
  }
}

case class TopicException() extends Exception