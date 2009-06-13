package deploylib

import scala.collection.mutable.ArrayBuffer
import org.json.JSONObject

import com.amazonaws.ec2._
import com.amazonaws.ec2.model._

object DataCenter {
  
  protected var instances: List[Instance] = List()
  
  private val configXML = xml.XML.loadFile("config.xml")
  
  private val accessKeyId     = (configXML \ "accessKeyId").text
  private val secretAccessKey = (configXML \ "secretAccessKey").text

  /* This method starts instances using the given arguments and returns
   * an InstanceGroup.
   * The EC2 access key ID and EC2 secret access key will be read from 
   * a configuration file. */
  def startInstances(imageId: String, count: Int, keyName: String,
                     instanceType: String, location: String):
                     Array[Instance] = {
    val service: AmazonEC2 = new AmazonEC2Client(accessKeyId, secretAccessKey)
    
    val request = new RunInstancesRequest()
    
    
    /* Poll until all instances are ready. */

    /* Turn instances into InstanceGroup */

    return null
  }
  
  def getInstanceGroupByTag(tag: String): InstanceGroup = {
    new InstanceGroup(instances.toArray)
  }
  
}
