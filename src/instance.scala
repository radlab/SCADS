package deploylib

import java.io.File
import java.io.InputStream
import org.json.JSONObject

import com.amazonaws.ec2._
import com.amazonaws.ec2.model._

class Instance(initialInstance: RunningInstance, keyPath: String) {
  var instance = initialInstance
  var ssh = null

  @throws(classOf[IllegalStateException])
  def deploy(config: JSONObject) = {
    checkSsh
  }
  
  @throws(classOf[IllegalStateException])
  def getCfg(): JSONObject = {
    checkSsh
    null
  }
  
  def stop = {
    
  }
  
  @throws(classOf[IllegalStateException])
  def getAllServices(): List[Service] = {
    checkSsh
    null
  }
  
  @throws(classOf[IllegalStateException])
  def getService(id: String): Option[Service] = {
    checkSsh
    None
  }
  
  @throws(classOf[IllegalStateException])
  def exec(cmd: String) = {
    checkSsh
  }
  
  /**
   * Wait for instance to be ready.
   */
  def waitUntilReady = {

  }
  
  private def refresh = {
    
  }
  
  private def checkSsh = {
    if (ssh == null){
      throw new IllegalStateException("Instance may not be ready yet. " +
                                      "Call waitUntilReady method first.")
    }
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
  
}

object Instance {
  object Type extends Enumeration {
    val m1_small  = Value("m1.small")
    val m1_large  = Value("m1.large")
    val m1_xlarge = Value("m1.xlarge")
    val c1_medium = Value("c1.medium")
    val c1_xlarge = Value("c1.xlarge")
  }
  
  def getValue(typeString: String): Type.Value = {
    Type.valueOf(typeString).get
  }
  
  def cores(instanceType: Type.Value): Int = instanceType  match {
    case Type.m1_small  => 1
    case Type.m1_large  => 2
    case Type.m1_xlarge => 4
    case Type.c1_medium => 2
    case Type.c1_xlarge => 8
  }
  
  def bits(instanceType: Type.Value): String = instanceType match {
    case Type.m1_small  => "32-bit"
    case Type.c1_medium => "32-bit"
    case _              => "64-bit"
  }
  
}
