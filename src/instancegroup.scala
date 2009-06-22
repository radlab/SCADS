package deploylib

import scala.actors._

class InstanceGroup(instances: List[Instance]) {
  def parallelExecute(fun: (Instance) => Unit): Unit = {
    
  }
  
  def parallelExecute(executer: InstanceExecute): Unit = {
    
  }
  
  def getInstance(id: String): Instance = {
    null
  }
  
  def ++(that: InstanceGroup): InstanceGroup = {
    new InstanceGroup(this.getInstances ++ that.getInstances)
  }
  
  def filter(p: (Instance) => Boolean): InstanceGroup = {
    new InstanceGroup(this.getInstances.filter(p))
  }
  
  private def getInstances = instances
}
