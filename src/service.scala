package deploylib

class Service(id: String) {
  
  def getId = id
  
  def start {
    Nil
  }
  
  def blockingStart {
    Nil
  }
  
  def once {
    Nil
  }
  
  def stop {
    Nil
  }
  
  def forceStop {
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
