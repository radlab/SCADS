package deploylib

class Service(id: String) {
  
  def start(): Unit = {
    Nil
  }
  
  def blockingStart(): Unit = {
    Nil
  }
  
  def once(): Unit = {
    Nil
  }
  
  def stop(): Unit = {
    Nil
  }
  
  def forceStop(): Unit = {
    Nil
  }
  
  def status(): String = {
    ""
  }
  
  def uptime(): Int = {
    0
  }
  
  def tailLog(): String = {
    ""
  }
  
}
