object DataCenter {
  
  val instances = new ArrayBuffer[Instance]()
  
  def startInstances(count: Int, type: Instance.Type): Array[Instance] {
    val ids = new Array[String](count)
    
    /* Request instances from EC2. */
    
    /* Poll until all instances are ready. */
    
    val instances = ids.map(id => new Instance(id))
    
    for (instance <- instances) {
      this.instances += instance
    }
    
    return instances
  }
  
}
