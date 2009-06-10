class Instance(id: String, internal_ip: String, external_ip: String) {
  
  val services = new ArrayBuffer[Service]()
  val tags = new ArrayBuffer[String]()
  
  def deploy(config: Map): Unit {
    
  }
  
  def stop(): Unit {
    
  }
  
  object Type extends Enumeration {
    val M1Small, M1Large, M1XLarge, C1Medium, C1XLarge = Value
  }
  
}
