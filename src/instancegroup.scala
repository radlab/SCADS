package deploylib

import scala.actors._

class InstanceGroup(instances: List[Instance]) {
  def parallelExecute(fun: (Instance) => Unit): Unit = {
    /* Place holder: serial execution */
    instances.foreach(instance => fun(instance))
  }
  
  def parallelExecute(executer: InstanceExecute): Unit = {
    /* Place holder: serial execution */
    instances.foreach(instance => executer.execute(instance))
  }
  
  def getInstance(id: String): Instance = {
    null
  }
  
  def getList = instances
}
