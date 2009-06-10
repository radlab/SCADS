import scala.collection.mutable.ArrayBuffer

class Instance(id: String, internal_ip: String, external_ip: String) {
  
  val services = new ArrayBuffer[Service]()
  val tags = new ArrayBuffer[String]()
  
  def deploy(config: Unit): Unit = {
    /* 'config' will need to be some sort of dictionary. */
    Nil
  }
  
  def stop(): Unit = {
    Nil
  }
  
  object Type extends Enumeration {
    val M1Small, M1Large, M1XLarge, C1Medium, C1XLarge = Value
  }
  
}
