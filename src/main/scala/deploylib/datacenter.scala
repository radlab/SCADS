package deploylib

import scala.collection.jcl.Conversions._

import java.io._
import scala.io.Source

import com.amazonaws.ec2._
import com.amazonaws.ec2.model._

/**
 * The DataCenter object has two main roles:
 * <ol>
 * <li> The abstraction to EC2.
 * <li> Keeping track of instances, so that they can queried over.
 * </ol>
 */
object DataCenter { 
  
  private val instances: InstanceGroup = new InstanceGroup()

  private val accessKeyId     = System.getenv.apply("AWS_ACCESS_KEY_ID")
  private val secretAccessKey = System.getenv.apply("AWS_SECRET_ACCESS_KEY")
  
  var keyName = System.getenv("AWS_KEY_NAME")
  var keyPath = System.getenv("AWS_KEY_PATH")
  
  private val config = new AmazonEC2Config()

  if(System.getenv.containsKey("EC2_URL"))
  	config.setServiceURL(System.getenv.apply("EC2_URL"))

  private val service = new AmazonEC2Client(accessKeyId, secretAccessKey, config)
  
  private val defaultDumpPath = System.getProperty("user.home") + "/.deploylib_state"
  
  /**
   * This method starts instances using the given arguments and returns
   * an InstanceGroup. Does not wait for instances to be running.
   *
   * @param imageId    the image id to use when deploying the instances
   * @param count      number of instances to startup
   * @param typeString the type of instance wanted ie. "m1.small", "c1.xlarge", etc.
   * @param location   the availability zone ie. "us-east-1a"
   * @return           An InstanceGroup object holding all instances allocated.
   *                   Note that the instances will not be in the ready state
   *                   when this method exits.
   */
  def runInstances(imageId: String, count: Int, typeString: String,
    location: String): InstanceGroup = {
    runInstances(imageId, count, typeString, location, false)
  }
  

  /**
   * This method starts instances using the given arguments and returns
   * an InstanceGroup.
   *
   * @param imageId        the image id to use when deploying the instances
   * @param count          number of instances to startup
   * @param typeString     the type of instance wanted ie. "m1.small", "c1.xlarge", etc.
   * @param location       the availability zone ie. "us-east-1a"
   * @param waitUntilReady if true will wait until instances are running before returning
   * @return           An InstanceGroup object holding all instances allocated.
   *                   Note that the instances will not be in the ready state
   *                   when this method exits.
   */
  def runInstances(imageId: String, count: Int, typeString: String,
    location: String, waitUntilReady: Boolean): InstanceGroup = {
    require(keyName != null,
      "DataCenter.keyName must be set either directly " + 
      "or by setting AWS_KEY_NAME environment variables before " +
      "calling this method.")
    
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
                                              new Instance(instance))
    val instanceGroup = new InstanceGroup(instanceList.toList)
    instances.addAll(instanceGroup)
    
    if (waitUntilReady) instanceGroup.waitUntilReady
    
    return instanceGroup
  }
  
  /**
   * Tells the DataCenter object about the instances in instanceGroup.
   */
  def addInstances(instanceGroup: InstanceGroup) {
    instances.addAll(instanceGroup)
  }
  /**
   * Tells the DataCenter object about the instance i.
   */
  def addInstances(i: Instance) {
    instances.add(i)
  }
  
  /**
   * Finds all instances known to DataCenter tagged with the specified tag.
   *
   * @param tag The tag to look for.
   * @return    An InstanceGroup containing instances that are all tagged with tag.
   */
  def getInstanceGroupByTag(tag: String): InstanceGroup = {
    instances.parallelFilter(instance => instance.isTaggedWith(tag))
  }
  
  // @TODO getInstanceGroupByService(service: Service)?
  
  /**
   * Finds all instances known to DataCenter that are running the specified service.
   *
   * @param service The service to look for.
   * @return        An InstanceGroup containing all instances that are running the specified service.
   */
  def getInstanceGroupByService(service: String): InstanceGroup = {
    instances.parallelFilter(instance => instance.getService(service).isDefined)
  }
  
  /**
   * Shuts down the given instances and returns them to Amazon.
   * Note: You should probably be using the instances' stop method, because it 
   * is easier to use and updates the state of the instance.
   */
  def terminateInstances(instanceGroup: InstanceGroup) = {
    val request = new TerminateInstancesRequest(
      convertScalaListToJavaList(instanceGroup.map(instance =>
        instance.instanceId).toList))
    service.terminateInstances(request)
    removeInstances(instanceGroup)
  }
  
  /**
   * Shuts down the given instance and returns it to Amazon.
   * Note: You should probably be using the instance's stop method, because it 
   * is easier to use and updates the state of the instance.
   */
  def terminateInstance(instance: Instance) = {
    val ig = new InstanceGroup()
    ig.add(instance)
    terminateInstances(ig)
  }
  
  /**
   * Shuts down all instances that are known to DataCenter.
   */
  def terminateAllInstances = {
    terminateInstances(instances)
    instances.parallelMap((instance) => instance.refresh)
    instances.clear()
  }
  
  /**
   * Removes the given instances from the DataCenter's knowledge of instances.
   * Note: This method is meant for internal use. Calling stop on an instance,
   * or calling terminateInstances calls this method automatically.
   */
  def removeInstances(instanceGroup: InstanceGroup) = {
    instances.removeAll(instanceGroup)
  }
  
  /**
   * Removes the given instance from the DataCenter's knowledge of instances.
   * Note: This method is meant for internal use. Calling stop on an instance,
   * or calling terminateInstances calls this method automatically.
   */
  def removeInstance(instance: Instance): Unit = {
    val instanceGroup = new InstanceGroup()
    instanceGroup.add(instance)
    removeInstances(instanceGroup)
  }
  
  /**
   * Runs describe instances through the EC2 library.
   * Note: This method is meant for internal use. The Instance class uses it to
   * refresh its state.
   */
  def describeInstances(idList: List[String]): List[RunningInstance] = {
    val request = new DescribeInstancesRequest(
      convertScalaListToJavaList(idList))
    val response = service.describeInstances(request)
    val result = response.getDescribeInstancesResult()
    val reservationList = result.getReservation()
    reservationList.toList.flatMap(reservation => reservation.getRunningInstance)
  }
  
  /**
   * Runs describe instances through the EC2 library.
   * Note: This method is meant for internal use. The Instance class uses it to
   * refresh its state.
   */
  def describeInstances(instances: InstanceGroup): List[RunningInstance] = {
    describeInstances(instances.map(instance => instance.instanceId).toList)
  }
  
  /**
   * Runs describe instances through the EC2 library.
   * Note: This method is meant for internal use. The Instance class uses it to
   * refresh its state.
   */
  def describeInstances(instance: Instance): RunningInstance = {
    describeInstances(List(instance.instanceId)).head
  }
  
  /**
   * Runs describe instances through the EC2 library.
   * Note: This method is meant for internal use. The Instance class uses it to
   * refresh its state.
   */
  def describeInstances(instanceId: String): RunningInstance = {
    describeInstances(List(instanceId)).head
  }
  
  /**
   * Writes the list of instance IDs belonging to the instances known to the
   * DataCenter object. The files are written to $HOME/.deploy_lib
   */
  def dumpStateToFile: Unit = {
    dumpStateToFile(null)
  }
  
  /**
   * Writes the list of instance IDs belonging to the instances known to the
   * DataCenter object.
   * @param path The path to the file to write to. If path is null or empty,
   *             then $HOME/.deploy_lib is used.
   */
  def dumpStateToFile(path: String): Unit = {
    val instanceIds = instances.map(instance => instance.instanceId)
    
    val filePath = path match {
      case null => defaultDumpPath
      case ""   => defaultDumpPath
      case _    => path
    }
    
    val out = new BufferedWriter(new FileWriter(filePath))
    
    try {
      for (id <- instanceIds) {
        out.write(id)
        out.newLine()
      }
    } finally {
      out.close()
    }
  }
  
  /**
   * Reconstructs the state by building instances from instance IDs specified
   * in a file. The file used is $HOME/.deploy_lib
   * 
   * @return        An InstanceGroup containing all instances constructed from the instance ids in the file.
   */
  def readStateFromFile: InstanceGroup = {
    readStateFromFile(null)
  }
  
  /**
   * Reconstructs the state by building instances from instance IDs specified
   * in a file. The file used is $HOME/.deploy_lib
   *
   * @param path    the path to the file to be read from. If path is null or empty $HOME/.deploy_lib is used.
   * @return        An InstanceGroup containing all instances constructed from the instance ids in the file.
   */
  def readStateFromFile(path: String): InstanceGroup = {
    val filePath = path match {
      case null => defaultDumpPath
      case ""   => defaultDumpPath
      case _    => path
    }
    
    val instanceIds = 
      for (id <- Source.fromFile(filePath).getLines.toList) yield id.trim
    
    val instanceList = 
      for (runningInstance <- describeInstances(instanceIds)) yield
        new Instance(runningInstance)
    val instanceGroup = new InstanceGroup(instanceList)
    instances.addAll(instanceGroup)
    
    return instanceGroup
  }
  
  private def convertScalaListToJavaList(aList:List[String]) =
    java.util.Arrays.asList(aList.toArray: _*)
  
}
