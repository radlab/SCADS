package edu.berkeley.cs.scads.storage

import net.lag.logging._

trait Namespace {
  def name: String
  def cluster: ScadsCluster

  /** Alias for name */
  def namespace: String = name

  protected val logger = Logger()

  trait Execable {
    def execCallbacks(): Unit
  }

  class CallbackHandler extends Execable {
    @volatile private var _functions: List[() => Unit] = Nil 
    def registerCallback(f: => Unit): Unit = {
      _functions ::= (() => f)
    }
    def execCallbacks(): Unit = _functions.foreach(f => f())
  }

  trait PassableCallbackHandler[A] extends Execable {
    def unit: A

    @volatile private var _functions: List[A => A] = Nil 
    def registerCallback(f: A => A): Unit = {
      _functions ::= f
    }
    def execCallbacks(): Unit = _functions.foldLeft(unit) { case (cur, f) => f(cur) }
  }

  private val createHandler = new CallbackHandler
  private val openHandler = new PassableCallbackHandler[Boolean] { val unit = false }
  private val closeHandler = new CallbackHandler
  private val deleteHandler = new CallbackHandler

  // NOTE: the handlers get appended in reverse order (like a stack). this
  // way, the callbacks for the dependencies run FIRST. 

  protected def onCreate(f: => Unit): Unit = createHandler.registerCallback(f) 
  protected def onOpen(f: Boolean => Boolean): Unit = openHandler.registerCallback(f)
  protected def onClose(f: => Unit): Unit = closeHandler.registerCallback(f)
  protected def onDelete(f: => Unit): Unit = deleteHandler.registerCallback(f)

  // TODO: what are the semantics for each? they should be specified clearly

  def create(): Unit = createHandler.execCallbacks()
  def open(): Unit = openHandler.execCallbacks()
  def close(): Unit = closeHandler.execCallbacks() 
  def delete(): Unit = deleteHandler.execCallbacks()

  override def toString = protToString
  protected def protToString: String = 
    "<Namespace: %s>".format(name)
}
