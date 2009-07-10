package deploylib

import scala.collection.mutable.ArrayBuffer
import scala.collection.jcl.Conversions._

import com.amazonaws.ec2._
import com.amazonaws.ec2.model._

object DataCenter {
  
  protected var instances: InstanceGroup = new InstanceGroup()

  private val accessKeyId     = System.getenv.apply("AWS_ACCESS_KEY_ID")
  private val secretAccessKey = System.getenv.apply("AWS_SECRET_ACCESS_KEY")

  private val config = new AmazonEC2Config()

  if(System.getenv.containsKey("EC2_URL"))
  	config.setServiceURL(System.getenv.apply("EC2_URL"))

  private val service = new AmazonEC2Client(accessKeyId, secretAccessKey, config)

  /**
   * This method starts instances using the given arguments and returns
   * an InstanceGroup.
   * The EC2 access key ID and EC2 secret access key will be read from 
   * a configuration file.
   *
   * @param imageId    the image id to use when deploying the instances
   * @param count      number of instances to startup
   * @param keyName    the name of the keypair to use
   * @param keyPath    the path to the local key file
   * @param typeString the type of instance wanted ie. "m1.small", "c1.xlarge", etc.
   * @param location   the availability zone ie. "us-east-1a"
   * @return           An InstanceGroup object holding all instances allocated.
   *                   Note that the instances will not be in the ready state
   *                   when this method exits.
   */
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
  
  def addInstances(instanceGroup: InstanceGroup) {
    instances.addAll(instanceGroup)
  }
  
  def addInstances(i: Instance) {
    instances.add(i)
  }
  
  def getInstanceGroupByTag(tag: String): InstanceGroup = {
    instances.parallelFilter(instance => instance.isTaggedWith(tag))
  }
  
  // @TODO getInstanceGroupByService(service: Service)?
  
  def getInstanceGroupByService(service: String): InstanceGroup = {
    instances.parallelFilter(instance => instance.getService(service).isDefined)
  }
  
  def terminateInstances(instanceGroup: InstanceGroup) = {
    val request = new TerminateInstancesRequest(
      convertScalaListToJavaList(instanceGroup.map(instance =>
        instance.instanceId).toList))
    service.terminateInstances(request)
    removeInstances(instanceGroup)
  }
  
  def terminateInstance(instance: Instance) = {
    val ig = new InstanceGroup()
    ig.add(instance)
    terminateInstances(ig)
  }
  
  def removeInstances(instanceGroup: InstanceGroup) = {
    instances.removeAll(instanceGroup)
  }
  
  def removeInstance(instance: Instance): Unit = {
    val instanceGroup = new InstanceGroup()
    instanceGroup.add(instance)
    removeInstances(instanceGroup)
  }
  
  def describeInstances(idList: List[String]): List[RunningInstance] = {
    val request = new DescribeInstancesRequest(
      convertScalaListToJavaList(idList))
    val response = service.describeInstances(request)
    val result = response.getDescribeInstancesResult()
    val reservationList = result.getReservation()
    reservationList.toList.flatMap(reservation => reservation.getRunningInstance)
  }
  
  def describeInstances(instances: InstanceGroup): List[RunningInstance] = {
    describeInstances(instances.map(instance => instance.instanceId).toList)
  }
  
  def describeInstances(instance: Instance): RunningInstance = {
    describeInstances(List(instance.instanceId)).head
  }
  
  def describeInstances(instanceId: String): RunningInstance = {
    describeInstances(List(instanceId)).head
  }
  
  def dumpStateToFile(path: Option[String]) = {
    
  }
  
  def readStateToFile(path: Option[String]) = {
    
  }
  
  private def convertScalaListToJavaList(aList:List[String]) =
    java.util.Arrays.asList(aList.toArray: _*)
  
}
