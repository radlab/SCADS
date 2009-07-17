package deploylib

import scala.actors._
import java.util.LinkedList
import scala.collection.jcl.Conversions._
import org.json.JSONObject

import scala.actors.Future
import scala.actors.Futures.future

/**
 * This class is the abstraction for a collection of instances.
 * It acts like java's LinkedList, since it inherits from it. This was done to
 * ensure compatibility with Java and Scala code. (Scala can interact with 
 * Java objects better than the other way around.)
 * <br>
 * If you are wrting Scala code I suggest that you import
 * scala.collection.jcl.Conversions._ so that you can use an InstanceGroup as
 * if it were a Scala list.
 */
class InstanceGroup(c: java.util.Collection[Instance])
  extends java.util.LinkedList[Instance](c) {

  /**
   * Creates an empty InstanceGroup.
   */
  def this() = this(new java.util.LinkedList[Instance]())
  
  /**
   * Populates this IntanceGroup with all instances in the List.
   */
  def this(list: List[Instance]) = {
    this(java.util.Arrays.asList(list.toArray: _*))
  }
  
  /**
   * Populates this InstanceGroup with all instances in the Array of InstanceGroups.
   * For Java compatibility.
   */
  def this(iterable: Array[InstanceGroup]) = {
    this()
    for (ig <- iterable) this.addAll(ig)
  }
  
  /**
   * Populates this InstanceGroup with all instances in the Iterable of InstanceGroups.
   */
  def this(iterable: Iterable[InstanceGroup]) = {
    this()
    for (ig <- iterable) this.addAll(ig)
  }

  /**
   * This function maps this collection in parallel using the given function.
   * <br>
   * It only works with Scala, since there are no function values in Java.
   * <br>
   * Adapted from:
   * http://debasishg.blogspot.com/2008/06/playing-around-with-parallel-maps-in.html
   */
  def parallelMap[T](fun: (Instance) => T): Array[T] = {
    val thisArray = new Array[Instance](this.size())
    this.toArray(thisArray)
    val resultArray = new Array[T](thisArray.length)
    val mappers = 
      for (i <- (0 until thisArray.length).toList) yield {
        future {
          resultArray(i) = fun(thisArray(i))
        }
      }
    for (mapper <- mappers) mapper()
    resultArray
  }
  
  /**
   * This function maps this collection in parallel using the given the execute
   * method in the given class.
   * <br>
   * This method was provided to give Java programmers a way to use parallelMap.
   * The problem is that the resulting array is not typed ie. it is an array of
   * Object. Let Aaron know if this is a huge nuisance.
   * <br>
   * Adapted from:
   * http://debasishg.blogspot.com/2008/06/playing-around-with-parallel-maps-in.html
   */
  def parallelMap(executer: InstanceExecute): Array[java.lang.Object] = {
    val thisArray = new Array[Instance](this.size())
    this.toArray(thisArray)
    val resultArray = new Array[java.lang.Object](thisArray.length)
    val mappers =
      for (i <- (0 until thisArray.length).toList) yield {
        scala.actors.Futures.future {
          resultArray(i) = executer.execute(thisArray(i))
        }
      }
    for (mapper <- mappers) mapper()
    resultArray
  }
  
  /**
   * This method works in parallel to return a new filtered InstanceGroup.
   * If applying the given function to an element of this InstanceGroup 
   * returns false then it will not be in the returned InstanceGroup.
   */
  def parallelFilter(fun: (Instance) => Boolean): InstanceGroup = {
    val zippedWithBools = this.toList zip parallelMap(fun).toList
    val filtered = 
      for (pair <- zippedWithBools if pair._2) yield pair._1
    new InstanceGroup(filtered)
  }
  
  /**
   * This method works in parallel to return a new filtered InstanceGroup.
   * If applying the given function to an element of this InstanceGroup 
   * returns false then it will not be in the returned InstanceGroup.
   * <br>
   * Make sure that the executer's execute function returns a java.lang.Boolean.
   */
  def parallelFilter(executer: InstanceExecute): InstanceGroup = {
    val zippedWithBools = this.toList zip parallelMap(executer).toList
    val filtered =
      for (pair <- zippedWithBools if pair._2.asInstanceOf[java.lang.Boolean].booleanValue) yield pair._1
    new InstanceGroup(filtered)
  }
  
  /**
   * Returns the first instance (wrapped in a Some) that is found with the
   * specified id. If no instance has the specified id, None is returned.
   */
  def getInstance(id: String): Option[Instance] = {
    this.find(instance => instance.instanceId == id)
  }
  
  /**
   * Runs deploy on each instance in parallel.
   */
  def deploy(config: JSONObject): Array[ExecuteResponse] = {
    deploy(config, null)
  }
  
  /**
   * Runs deploy on each instance in parallel.
   */
  def deploy(config: JSONObject, repoPath: String): Array[ExecuteResponse] = {
    parallelMap((instance) => instance.deploy(config, repoPath))
  }
  
  /**
   * Runs deployNonBlocking on each instance.
   */
  def deployNonBlocking(config: JSONObject): Future[Array[ExecuteResponse]] = {
    deployNonBlocking(config, null)
  }
  
  /**
   * Runs deployNonBlocking on each instance.
   */
  def deployNonBlocking(config: JSONObject, repoPath: String): Future[Array[ExecuteResponse]] = {
    val thisArray = new Array[Instance](this.size())
    this.toArray(thisArray)
    val deployments: Array[Future[ExecuteResponse]] =
      for (instance <- thisArray)
        yield instance.deployNonBlocking(config, repoPath)
    future {
      for (deployment <- deployments) yield deployment()
    }
  }
  
  def stopAll = { 
    DataCenter.terminateInstances(this)
    refreshAll
  }
  
  def cleanServices = { parallelMap((instance) => instance.cleanServices) }
  
  def tagWith(tag: String) = { parallelMap((instance) => instance.tagWith(tag)) }
  
  def removeTag(tag: String) = { parallelMap((instance) => instance.removeTag(tag)) }
  
  def exec(cmd: String): Array[ExecuteResponse] = { 
    parallelMap((instance) => instance.exec(cmd))
  }
  
  def waitUntilReady = { parallelMap((instance) => instance.waitUntilReady) }
  
  def refreshAll = {
    val instancePairs = this.toList zip DataCenter.describeInstances(this)
    for (pair <- instancePairs) pair._1.runningInstance = pair._2
  }
  
  def allRunning: Boolean = { this.forall((instance) => instance.running) }
  
  def allTerminated: Boolean = { this.forall((instance) => instance.terminated) }
}
