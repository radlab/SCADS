package deploylib
package ec2

import com.amazonaws.services.sns._
import com.amazonaws.services.sns.model._

object ExperimentNotification extends AWSConnection {
  val snsClient = new AmazonSNSClient(credentials)
  
  protected class Topic(topicText: String) {
    val topicArn = snsClient.createTopic(new CreateTopicRequest(topicText)).getTopicArn
    
    def subscribeViaEmail(emailAddress: String): Unit =
      snsClient.subscribe(new SubscribeRequest(topicArn, "email", emailAddress))
    
    def publish(subject: String, message: String): Unit =
      snsClient.publish(new PublishRequest(topicArn, message, subject))
  }

  object completions extends Topic("completions")
  object failures extends Topic("failures")
}
