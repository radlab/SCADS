package deploylib

import scala.collection.mutable.ArrayBuffer
import org.json.JSONObject

class Instance(instanceId:        String,
               imageId:           String,
           var instanceState:     String,
               privateDnsName:    String,
               publicDnsName:     String,
               keyName:           String,
               instanceType:      Instance.Type.Value,
               launchTime:        String,
               availabilityZone:  String) {  
  
  val services = new ArrayBuffer[Service]()
  val tags     = new ArrayBuffer[String]()
  
  def deploy(config: JSONObject): Unit = {
    /* 'config' will need to be some sort of dictionary. */
    Nil
  }
  
  def getCfg(): JSONObject = {
    new JSONObject()
  }
  
  def stop(): Unit = {
    Nil
  }
  
  def getAllServices(): ArrayBuffer[Service] = {
    services
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
}
