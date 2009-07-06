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
    DataCenter.addInstances(this)
  }

  @throws(classOf[IllegalStateException])
  def deploy(config: JSONObject): Array[Service] = {
    checkSsh
    exec("cd && echo \'" + config.toString() + "\' > config.js && " +
         "chef-solo -j config.js")
    getAllServices
  }
  
  @throws(classOf[IllegalStateException])
  def getCfg(): Option[JSONObject] = {
    checkSsh
    val response = exec("cd && cat config.js")
    if (Util.responseError(response))
      return None
    else
      return Some(new JSONObject(response.getStdout()))
  }
  
  def stop = {
    DataCenter.terminateInstances(new InstanceGroup(List(this)))
  }
  
  @throws(classOf[IllegalStateException])
  def getAllServices: Array[Service] = {
    checkSsh
    val response = exec("ls /mnt/services")
    if (Util.responseError(response))
      return Array()
    else {
      return response.getStdout.split("\n").
                map(service => new Service(service, this))
    }
  }
  
  @throws(classOf[IllegalStateException])
  def getService(id: String): Option[Service] = {
    getAllServices.find(service => service.getId == id)
  }
  
  def tagWith(tag: String) = {
    checkSsh
    exec("echo \'" + tag + "\' >> /mnt/tags")
  }
  
  def isTaggedWith(tag: String): Boolean = {
    getAllTags.find(possible => possible == tag).isDefined
  }
  
  def getAllTags: Array[String] = {
    checkSsh
    val response = exec("cat /mnt/tags")
    if (Util.responseError(response)) return Array()
    else return response.getStdout.split("\n")
  }
  
  @throws(classOf[IllegalStateException])
  def exec(cmd: String): ExecuteResponse = {
    checkSsh
    ssh.executeCommand(cmd)
  }
  
  def waitUntilReady: Unit = {
    if (terminated) {
      DataCenter.removeInstance(this)
      return
    }
    while (!running) {
      refresh
      Thread.sleep(5000)
    }
    ssh = new SSH(publicDnsName, keyPath)
  }
  
  private def refresh = {
    instance = DataCenter.describeInstances(this)
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