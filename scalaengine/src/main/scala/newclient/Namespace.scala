package edu.berkeley.cs.scads.storage.newclient

import edu.berkeley.cs.scads.storage.ScadsCluster
import net.lag.logging._

trait Namespace {
  def name: String
  def cluster: ScadsCluster

  protected val logger = Logger()

  class CallbackHandler {
    @volatile private var _functions: List[() => Unit] = Nil 
    def registerCallback(f: => Unit): Unit = {
      _functions ::= (() => f)
    }
    def execCallbacks(): Unit = _functions.foreach(f => f())
  }

  private val createHandler = new CallbackHandler
  private val openHandler = new CallbackHandler
  private val closeHandler = new CallbackHandler
  private val deleteHandler = new CallbackHandler

  // NOTE: the handlers get appended in reverse order (like a stack). this
  // way, the callbacks for the dependencies run FIRST. 

  protected def onCreate(f: => Unit): Unit = createHandler.registerCallback(f) 
  protected def onOpen(f: => Unit): Unit = openHandler.registerCallback(f)
  protected def onClose(f: => Unit): Unit = closeHandler.registerCallback(f)
  protected def onDelete(f: => Unit): Unit = deleteHandler.registerCallback(f)

  // TODO: what are the semantics for each? they should be specified clearly

  def create(): Unit = createHandler.execCallbacks()
  def open(): Unit = openHandler.execCallbacks()
  def close(): Unit = closeHandler.execCallbacks() 
  def delete(): Unit = deleteHandler.execCallbacks()
}
