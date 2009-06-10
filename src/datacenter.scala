import scala.collection.mutable.ArrayBuffer

object DataCenter {
  
  val instances = new ArrayBuffer[Instance]()
  
  def startInstances(count: Int, instanceType: Instance.Type): Array[Instance] = {
    val ids = new Array[String](count)
    
    /* Request instances from EC2. */
    
    /* Poll until all instances are ready. */
    
    val instances = ids.map(id => new Instance(id))
    
    this.instances ++= instances

    return instances
  }
  
}
