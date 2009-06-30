package deploylib

import scala.actors._
import java.util.LinkedList
import scala.collection.jcl.Conversions._

class InstanceGroup(c: java.util.Collection[Instance])
  extends java.util.LinkedList[Instance](c) {

  def this() = this(new java.util.LinkedList[Instance]())
  
  def this(list: List[Instance]) = {
    this(java.util.Arrays.asList(list.toArray: _*))
  }

  def parallelExecute(fun: (Instance) => Unit): Unit = {
    /* Place holder: serial execution */
    this.foreach(instance => fun(instance))
  }
  
  def parallelExecute(executer: InstanceExecute): Unit = {
    /* Place holder: serial execution */
    this.foreach(instance => executer.execute(instance))
  }
  
  def getInstance(id: String): Instance = {
    null
  }
  
}
