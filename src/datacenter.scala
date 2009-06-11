package deploylib

import scala.collection.mutable.ArrayBuffer
import org.json.JSONObject

object DataCenter {
  
  val instances = new ArrayBuffer[Instance]()
  
  def startInstances(count: Int, instanceType: String): Array[Instance] = {
    val ids = new Array[String](count)
    
    /* Request instances from EC2. */
    
    /* Poll until all instances are ready. */
    
    val instances = ids.map(id => new Instance(id, "imageId", "instanceState",
        "privateDns", "publicDns", "keyName", "instanceTyep", "launchTime",
        "availabilityZone"))
    
    this.instances ++= instances

    return instances
  }
  
}
