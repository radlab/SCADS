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
  
  
  /**
   * Wait for instance to be ready.
   */
  def waitUntilReady(): Unit = {

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
