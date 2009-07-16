package deploylib

import java.io.File
import java.io.InputStream
import org.json.JSONObject

import com.amazonaws.ec2._
import com.amazonaws.ec2.model._

import scala.actors.Future
import scala.actors.Futures.future

/**
 * This class is the abstraction to a running EC2 Instance, as such it has
 * methods to interact with the EC2 Instance.
 * <br>
 * <br>
 * There are two things to note:
 * <br>
 * (One) Any method requiring an SSH connection will are
 * not able to execute unless the instance is in a running state and contains
 * an SSH object.
 * <br>
 * <br>
 * There are two ways that an instance can get an SSH object. Either the
 * instance is instantiated with an EC2 instance is running, or waitUntilReady
 * method is called.
 * <br>
 * <br>
 * (Two) The state of the instance is frozen from whenever this instance
 * object was instantiated. ie. The response to a running call will always be the
 * same unless you update the state. To update the state call refresh on the instance
 * object.
 */
class Instance(initialInstance: RunningInstance, keyPath: String) {
  private var instance = initialInstance
  private var ssh: SSH = null
  if (running) ssh = new SSH(publicDnsName, keyPath)
  
  /**
   * This alternate constructor is for the ease of creating instance objects
   * from already running instances.
   * Maybe this should be moved to DataCenter...
   */
  def this(instanceId: String, keyPath: String) {
    this(DataCenter.describeInstances(instanceId), keyPath)
    if (running) ssh = new SSH(publicDnsName, keyPath)
    if (!terminated) DataCenter.addInstances(this)
  }

  /**
   * Calls chef-solo with the default chef-repo.
   *
   * @param config This is the configuration passed to chef-solo.
   * @return       What appeared on the shell in the form of an ExecuteResponse.
   */
  @throws(classOf[IllegalStateException])
  def deploy(config: JSONObject): ExecuteResponse = {
    deploy(config, null)
  }
  
  /**
   * Runs chef-solo on the instance.
   *
   * @param config   This is the configuration passed to chef-solo.
   * @param repoPath This is the path to a chef-repo gzipped tarball. The path
   *                 may be a URL or a path to a chef-repo local to the instance.
   *                 If repoPath is empty or null, the current head of the git
   *                 repository is used.
   * @return         What appeared on the shell in the form of an ExecuteResponse.
   */
  @throws(classOf[IllegalStateException])
  def deploy(config: JSONObject, repoPath: String): ExecuteResponse = {
    if (repoPath == null || repoPath.length() == 0)
      exec("echo \'" + config.toString() + "\' > config.js && " +
        "chef-solo -j config.js")
    else
      exec("echo \'" + config.toString() + "\' > config.js && " +
        "chef-solo -j config.js -r " + repoPath)
  }
  
  def deployNonBlocking(config: JSONObject): Future[ExecuteResponse] = {
    deployNonBlocking(config, null)
  }
  
  def deployNonBlocking(config: JSONObject, repoPath: String): Future[ExecuteResponse] = {
    scala.actors.Futures.future { deploy(config, repoPath) }
  }
  
  /**
   * Creates a JSONObject out of the text in ~/config.js residing on the instance.
   *
   * @return None if there was an error reading config.js, Some(JSONObject)
   *         otherwise.
   */
  @throws(classOf[IllegalStateException])
  def getCfg(): Option[JSONObject] = {
    val response = exec("cd && cat config.js")
    if (response error)
      return None
    else
      return Some(new JSONObject(response.getStdout()))
  }
  
  /**
   * Shuts down this instance and removes itself from the DataCenter's list of 
   * instances.
   */
  def stop = {
    DataCenter.terminateInstances(new InstanceGroup(List(this)))
    refresh
  }
  
  /**
   * Makes a service object out of all runit services installed on the instance.
   */
  @throws(classOf[IllegalStateException])
  def getAllServices: Array[Service] = {
    val response = exec("ls /mnt/services")
    if (response error)
      return Array()
    else {
      return response.getStdout.split("\n").
                map(service => new Service(service, this))
    }
  }
  
  /**
   * Removes all runit services.
   */
  def cleanServices = {
    exec("rm -rf /mnt/services")
  }
  
  /**
   * If the service of the corresponding name exists, returns it wrapped in
   * a Some object otherwise returns None.
   */
  @throws(classOf[IllegalStateException])
  def getService(id: String): Option[Service] = {
    getAllServices.find(service => service.getId == id)
  }
  
  /**
   * Tags the instance with the given tag. This tag persists with the EC2
   * instance.
   */
  @throws(classOf[IllegalStateException])
  def tagWith(tag: String) = {
    exec("echo \'" + tag + "\' >> /mnt/tags")
  }

  /**
   * Checks whether the instance is tagged with the given tag.
   */
  @throws(classOf[IllegalStateException])  
  def isTaggedWith(tag: String): Boolean = {
    getAllTags.find(possible => possible == tag).isDefined
  }
  
  /**
   * Gets all tags the instance has been tagged with.
   */
  @throws(classOf[IllegalStateException])
  def getAllTags: Array[String] = {
    val response = exec("cat /mnt/tags")
    if (response error) return Array()
    else return response.getStdout.split("\n")
  }
  
  /**
   * Removes the given tag from the instance.
   */
  @throws(classOf[IllegalStateException])
  def removeTag(tag: String) ={
    exec("sed \'/" + tag + "/d\' /mnt/tags > /mnt/tmp && mv /mnt/tmp /mnt/tags")
  }
  
  /**
   * Executes the given command on the instance.
   *
   * @throws IllegalStateException if SSH has not been set up. See the
   *                               Instance class documentation for a discussion of this.
   */
  @throws(classOf[IllegalStateException])
  def exec(cmd: String): ExecuteResponse = {
    checkSsh
    ssh.executeCommand(cmd)
  }
  
  /**
   * Blocks the current thread until this instance is in running state.
   */
  def waitUntilReady: Unit = {
    while (refresh && !running) {
      Thread.sleep(5000)
    }
    ssh = new SSH(publicDnsName, keyPath)
  }
  
  /**
   * Updates the instance status by checking with EC2.
   * If the instance is shutting down or is terminated then it will be removed
   * from the DataCenter's list of instances.
   *
   * @return The method returns false if the instance is terminated or in the
   *         process of being terminated, true otherwise.
   */
  def refresh: Boolean = {
    instance = DataCenter.describeInstances(this)
    if (terminated) {
      DataCenter.removeInstance(this)
      return false
    }
    return true
  }
  
  private def checkSsh = {
    if (ssh == null){
      throw new IllegalStateException("Instance may not be ready yet. " +
                                      "Call waitUntilReady method first.")
    }
  }
  /**
   * Returns true if the instance is in a running state, false otherwise.
   */
  def running: Boolean = {
    instanceState == "running"
  }
  
  /**
   * Returns true if the instance is in the state of 'shutting-down' or 'terminated'.
   */
  def terminated: Boolean = {
    instanceState == "shutting-down" || instanceState == "terminated"
  }
  
  /** Returns the instance ID. */
  def instanceId: String = {
    instance.getInstanceId()
  }
  
  /** Returns the image ID. */
  def imageId: String = {
    instance.getImageId()
  }
  
  /** Returns the isntance state. */
  def instanceState: String = {
    instance.getInstanceState().getName()
  }
  
  /** Returns the private DNS name. */
  def privateDnsName: String = {
    instance.getPrivateDnsName()
  }
  
  /** Returns the public DNS name. */
  def publicDnsName: String = {
    instance.getPublicDnsName()
  }
  
  /** Returns the public key name. */  
  def keyName: String = {
    instance.getKeyName()
  }
  
  /** Returns the instance type. */
  def instanceType: String = {
    instance.getInstanceType()
  }
  
  /** Returns the time the instance was launched. */
  def launchTime: String = {
    instance.getLaunchTime()
  }
  
  /** Returns the availability zone the instance resides in. */
  def availabilityZone: String = {
    instance.getPlacement().getAvailabilityZone()
  }
  
  override def equals(other: Any): Boolean = other match {
    case that: Instance => instanceId.equals(that.instanceId)
    case _ => false
  }
  
  override def hashCode: Int = instanceId.hashCode
}