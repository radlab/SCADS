package deploylib

import scala.collection.mutable.ArrayBuffer
import org.json.JSONObject

class Instance(instanceId:        String,
               imageId:           String,
           var instanceState:     String,
               privateDnsName:    String,
               publicDnsName:     String,
               keyName:           String,
               instanceType:      String,
               launchTime:        String,
               availabilityZone:  String) {  
  
  val services = new ArrayBuffer[Service]()
  val tags     = new ArrayBuffer[String]()
  
  def deploy(config: JSONObject): Unit = {
    /* 'config' will need to be some sort of dictionary. */
    Nil
  }
  
  def stop(): Unit = {
    Nil
  }
  
}
