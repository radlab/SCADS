package deploylib

import scala.collection.mutable.ArrayBuffer
import org.json.JSONObject

import com.amazonaws.ec2._
import com.amazonaws.ec2.model._

object DataCenter {
  
  val instances = new ArrayBuffer[Instance]()
  
  def startInstances(count: Int, instanceType: String): Array[Instance] = {
    val ids = new Array[String](count)
    
    /* Request instances from EC2. */
    val accessKeyId = "<Access Key ID>"
    val secretAccessKey = "<Secret Access Key>"
    
    val service: AmazonEC2 = new AmazonEC2Client(accessKeyId, secretAccessKey)
    
    
    /* Poll until all instances are ready. */
    
    val instances = ids.map(id => new Instance(id, "imageId", "instanceState",
        "privateDns", "publicDns", "keyName", Instance.Type.m1_small,
        "launchTime", "availabilityZone"))
    
    this.instances ++= instances

    return instances
  }
  
  def getInstanceGroupByTag(tag: String): InstanceGroup = {
    new InstanceGroup(instances.toArray)
  }
  
}
