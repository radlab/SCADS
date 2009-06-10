import scala.collection.mutable.ArrayBuffer

object DataCenter {
  
  val instances = new ArrayBuffer[EC2Instance]()
  
  def startInstances(count: Int, instanceType: EC2Instance.Type): Array[EC2Instance] = {
    val ids = new Array[String](count)
    
    /* Request instances from EC2. */
    
    /* Poll until all instances are ready. */
    
    val instances = ids.map(id => new EC2Instance(id))
    
    this.instances ++= instances

    return instances
  }
  
}
