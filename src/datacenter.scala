package deploylib

import scala.collection.mutable.ArrayBuffer
import scala.collection.jcl.Conversions._
import org.json.JSONObject

import com.amazonaws.ec2._
import com.amazonaws.ec2.model._

object DataCenter {
  
  protected var instances: InstanceGroup = new InstanceGroup()
  
  private val configXML = xml.XML.loadFile("config.xml")
  
  private val accessKeyId     = (configXML \ "accessKeyId").text
  private val secretAccessKey = (configXML \ "secretAccessKey").text
  
  private val service = new AmazonEC2Client(accessKeyId, secretAccessKey)

  /* This method starts instances using the given arguments and returns
   * an InstanceGroup.
   * The EC2 access key ID and EC2 secret access key will be read from 
   * a configuration file. */
  def runInstances(imageId: String, count: Int, keyName: String,
                   keyPath: String, typeString: String,
                   location: String):
                   InstanceGroup = {
    
    InstanceType.checkValidType(typeString)
    
    val request = new RunInstancesRequest(
                        imageId,                 // imageID
                        count,                   // minCount
                        count,                   // maxCount
                        keyName,                 // keyName
                        null,                    // securityGroup
                        null,                    // userData
                        typeString,              // instanceType
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
    val instanceGroup = new InstanceGroup(instanceList.toList)
    instances.addAll(instanceGroup)
    
    return instanceGroup
  }
  
  def getInstanceGroupByTag(tag: String): InstanceGroup = {
    val filtered = instances.filter(instance =>
           instance.getService(tag).isDefined)
    new InstanceGroup(filtered.toList)
  }
  
  def terminateInstances(instanceGroup: InstanceGroup) = {
    /* TODO: Do API stuff to actually stop running instances. */
    removeInstances(instanceGroup)
  }
  
  def removeInstances(instanceGroup: InstanceGroup) = {
    instances.removeAll(instanceGroup)
  }
  
  def removeInstance(instance: Instance): Unit = {
    removeInstance(instance.instanceId)
  }
  
  def removeInstance(instanceId: String): Unit = {
    
  }
  
  def describeInstances(idList: List[String]) = {
    val request = new DescribeInstancesRequest(
                                    convertScalaListToJavaList(idList.toList))
    val response = service.describeInstances(request)
    val result = response.getDescribeInstancesResult()
    val reservationList = result.getReservation()
    reservationList.toList.flatMap(reservation => reservation.getRunningInstance)
  }
  
  def describeInstances(instances: InstanceGroup): List[RunningInstance] = {
    describeInstances(instances.map(instance => instance.instanceId).toList)
  }
  
  private def convertScalaListToJavaList(aList:List[String]) =
    java.util.Arrays.asList(aList.toArray: _*)
  
}
