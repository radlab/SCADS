package deploylib

import java.io.File
import java.io.InputStream
import org.json.JSONObject

import com.amazonaws.ec2._
import com.amazonaws.ec2.model._

class Instance(initialInstance: RunningInstance, keyPath: String) {
  var instance = initialInstance
  var ssh: SSH = null
  
  def this(instanceId: String, keyPath: String) {
    this(DataCenter.describeInstances(instanceId), keyPath)
    if (running) ssh = new SSH(publicDnsName, keyPath)
    if (!terminated) DataCenter.addInstances(this)
  }

  @throws(classOf[IllegalStateException])
  def deploy(config: JSONObject): ExecuteResponse = {
    exec("cd && echo \'" + config.toString() + "\' > config.js && " +
         "chef-solo -j config.js")
  }
  
  @throws(classOf[IllegalStateException])
  def getCfg(): Option[JSONObject] = {
    val response = exec("cd && cat config.js")
    if (Util.responseError(response))
      return None
    else
      return Some(new JSONObject(response.getStdout()))
  }
  
  def stop = {
    DataCenter.terminateInstances(new InstanceGroup(List(this)))
    refresh
  }
  
  @throws(classOf[IllegalStateException])
  def getAllServices: Array[Service] = {
    val response = exec("ls /mnt/services")
    if (Util.responseError(response))
      return Array()
    else {
      return response.getStdout.split("\n").
                map(service => new Service(service, this))
    }
  }
  
  def cleanServices = {
    exec("rm -rf /mnt/services")
  }
  
  @throws(classOf[IllegalStateException])
  def getService(id: String): Option[Service] = {
    getAllServices.find(service => service.getId == id)
  }
  
  @throws(classOf[IllegalStateException])
  def tagWith(tag: String) = {
    exec("echo \'" + tag + "\' >> /mnt/tags")
  }

  @throws(classOf[IllegalStateException])  
  def isTaggedWith(tag: String): Boolean = {
    getAllTags.find(possible => possible == tag).isDefined
  }
  
  @throws(classOf[IllegalStateException])
  def getAllTags: Array[String] = {
    val response = exec("cat /mnt/tags")
    if (Util.responseError(response)) return Array()
    else return response.getStdout.split("\n")
  }
  
  @throws(classOf[IllegalStateException])
  def removeTag(tag: String) ={
    exec("sed \'/" + tag + "/d\' /mnt/tags > /mnt/tmp && mv /mnt/tmp /mnt/tags")
  }
  
  @throws(classOf[IllegalStateException])
  def exec(cmd: String): ExecuteResponse = {
    checkSsh
    ssh.executeCommand(cmd)
  }
  
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
  
  def running: Boolean = {
    instanceState == "running"
  }
  
  def terminated: Boolean = {
    instanceState == "shutting-down" || instanceState == "terminated"
  }
  
  /* Accessors */
  def instanceId: String = {
    instance.getInstanceId()
  }
  
  def imageId: String = {
    instance.getImageId()
  }
  
  def instanceState: String = {
    instance.getInstanceState().getName()
  }
  
  def privateDnsName: String = {
    instance.getPrivateDnsName()
  }
  
  def publicDnsName: String = {
    instance.getPublicDnsName()
  }
  
  def keyName: String = {
    instance.getKeyName()
  }
  
  def instanceType: String = {
    instance.getInstanceType()
  }
  
  def launchTime: String = {
    instance.getLaunchTime()
  }
  
  def availabilityZone: String = {
    instance.getPlacement().getAvailabilityZone()
  }
  
  override def equals(other: Any): Boolean = other match {
    case that: Instance => instanceId.equals(that.instanceId)
    case _ => false
  }
  
  override def hashCode: Int = instanceId.hashCode
}