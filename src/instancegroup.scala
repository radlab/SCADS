package deploylib

import scala.actors._

class InstanceGroup(instances: Array[Instance]) {
  def parallelExecute(fun: (Instance) => Unit): Unit = {
    
  }
  
  def getInstance(id: String): Instance = {
    instances(0)
  }
}
