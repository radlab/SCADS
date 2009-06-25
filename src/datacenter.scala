package deploylib

import scala.collection.mutable.ArrayBuffer
import scala.collection.jcl.Conversions._
import org.json.JSONObject

import com.amazonaws.ec2._
import com.amazonaws.ec2.model._

object DataCenter {
  
  protected var instances: InstanceGroup = new InstanceGroup(Nil)
  
  private val configXML = xml.XML.loadFile("config.xml")
  
  private val accessKeyId     = (configXML \ "accessKeyId").text
  private val secretAccessKey = (configXML \ "secretAccessKey").text
  
  private val service = new AmazonEC2Client(accessKeyId, secretAccessKey)

  /* This method starts instances using the given arguments and returns
   * an InstanceGroup.
   * The EC2 access key ID and EC2 secret access key will be read from 
   * a configuration file. */
  def runInstances(imageId: String, count: Int, keyName: String,
                   keyPath: String, instanceType: Instance.Type.Value,
                   location: String):
                   InstanceGroup = {
    
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
                        
    val response: RunInstancesResponse = service.runInstances(request)
    val result: RunInstancesResult = response.getRunInstancesResult()
    val reservation: Reservation = result.getReservation()
    
    val runningInstanceList = reservation.getRunningInstance()
    
    val instanceList = runningInstanceList.map(instance =>
                                               new Instance(instance, keyPath))
    
    instances = new InstanceGroup(instances.getList ++ instanceList)
    
    return new InstanceGroup(instanceList.toList)
  }
  
  def getInstanceGroupByTag(tag: String): InstanceGroup = {
    new InstanceGroup(instances.getList.
      filter(instance => instance.getService(tag).isDefined))
  }
  
  def terminateInstances(instances: InstanceGroup) = {
    
  }
  
  def describeInstances(instances: InstanceGroup): List[RunningInstance] = {
    val idList = instances.getList.map(instance => instance.instanceId)
    val request = new DescribeInstancesRequest(
                                          convertScalaListToJavaList(idList))
    val response = service.describeInstances(request)
    val result = response.getDescribeInstancesResult()
    val reservationList = result.getReservation()
    reservationList.toList.flatMap(reservation => reservation.getRunningInstance)
  }
  
  private def convertScalaListToJavaList(aList:List[String]) =
    java.util.Arrays.asList(aList.toArray: _*)
  
}
