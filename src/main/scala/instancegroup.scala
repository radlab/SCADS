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

  def parallelMap[T](fun: (Instance) => T): Array[T] = {
    /* Adapted from:
     * http://debasishg.blogspot.com/2008/06/playing-around-with-parallel-maps-in.html*/
    val thisArray = new Array[Instance](this.size())
    this.toArray(thisArray)
    val resultArray = new Array[T](thisArray.length)
    val mappers = 
      for (i <- (0 until thisArray.length).toList) yield {
        scala.actors.Futures.future {
          resultArray(i) = fun(thisArray(i))
        }
      }
    for (mapper <- mappers) mapper()
    resultArray
  }
  
  def parallelMap[T](executer: InstanceExecute[T]): Array[T] = {
    /* Place holder: serial execution */
    this.foreach(instance => executer.execute(instance))
    
    val thisArray = new Array[Instance](this.size())
    this.toArray(thisArray)
    val resultArray = new Array[T](thisArray.length)
    val mappers =
      for (i <- (0 until thisArray.length).toList) yield {
        scala.actors.Futures.future {
          resultArray(i) = executer.execute(thisArray(i))
        }
      }
    for (mapper <- mappers) mapper()
    resultArray
  }
  
  def parallelFilter(fun: (Instance) => Boolean): InstanceGroup = {
    // TODO
    null
  }
  
  def parallelFilter(executer: InstanceExecute[Boolean]): InstanceGroup = {
    // TODO
    null
  }
  
  def getInstance(id: String): Option[Instance] = {
    this.find(instance => instance.instanceId == id)
  }
  
}
