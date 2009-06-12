package deploylib

import scala.collection.mutable.ArrayBuffer
import org.json.JSONObject

import com.amazonaws.ec2._
import com.amazonaws.ec2.model._

class Instance(instance: RunningInstance) { 
                 
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
  
  def getAllServices(): List[Service] = {
    List(new Service("Skeleton"))
  }
  
  def getService(id: String): Service = {
    new Service("Skeleton")
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
