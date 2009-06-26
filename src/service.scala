package deploylib

class Service(id: String, instance: Instance) {
  
  def getId = id
  
  def start {
    instance.exec("sv start /mnt/services/" + id)
  }
  
  def blockingStart(wait: Int): Unit = {

  }
  
  def blockingStart(): Unit = {
    blockingStart(30)
  }
  
  def once {
    instance.exec("sv once /mnt/services/" + id)
  }
  
  def stop {
    instance.exec("sv stop /mnt/services/" + id)
  }
  
  def forceStop {
    instance.exec("sv force-stop /mnt/services/" + id)
  }
  
  def status(): ServiceStatus = {
    new ServiceStatus("", "", 0, 0)
  }
  
  def tailLog(): String = {
    ""
  }
  
}
