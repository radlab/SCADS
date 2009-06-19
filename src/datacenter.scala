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
  def runInstances(imageId: String, count: Int, keyName: String,
                   instanceType: Instance.Type.Value, location: String):
                   InstanceGroup = {
    val service: AmazonEC2 = new AmazonEC2Client(accessKeyId, secretAccessKey)
    
    val request = new RunInstancesRequest(
                        imageId,                 // imageID
                        count,                   // minCount
                        count,                   // maxCount
                        keyName,                 // keyName
                        null,                    // securityGroup
                        null,                    // userData
                        instanceType.toString,   // instanceType
                        new Placement(location), // placement
                        null,                    // kernelId
                        null,                    // ramdiskId
                        null,                    // blockDeviceMapping
                        null                     // monitoring
                        )
    
    
    /* Turn instances into InstanceGroup */

    return null
  }
  
  def getInstanceGroupByTag(tag: String): InstanceGroup = {
    null
  }
  
}
