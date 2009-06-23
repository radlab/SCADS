package deploylib

import scala.collection.mutable.ArrayBuffer
import org.json.JSONObject

import com.sshtools.j2ssh.SshClient
import com.sshtools.j2ssh.transport.IgnoreHostKeyVerification
import com.sshtools.j2ssh.transport.publickey.SshPrivateKeyFile
import com.sshtools.j2ssh.authentication.PublicKeyAuthenticationClient

import com.amazonaws.ec2._
import com.amazonaws.ec2.model._

class Instance(instance: RunningInstance) {
  var chefConfig: JSONObject = null
  var ssh: SshClient = null
                 
  def deploy(config: JSONObject): Unit = {
    /* 'config' will need to be some sort of dictionary. */
    chefConfig = config
    startSsh
    ssh.connect(publicDnsName, new IgnoreHostKeyVerification())
    
  }
  
  def getCfg(): JSONObject = {
    chefConfig
  }
  
  def stop(): Unit = {
    null
  }
  
  def getAllServices(): List[Service] = {
    null
  }
  
  def getService(id: String): Option[Service] = {
    None
  }
  
  /**
   * Wait for instance to be ready.
   */
  def waitUntilReady(): Unit = {

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
  
  private def startSsh = {
    if (ssh == null) {
      ssh = new SshClient()
    }
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
