package edu.berkeley.cs.scads.storage

import net.lag.logging._

trait Namespace {
  def name: String
  def cluster: ScadsCluster

  /** Alias for name */
  def namespace: String = name

  /** Default to building bdb namespaces.  Subclasses can override this
   ** for other types if they want */
  def partitionType: String = "bdb"

  protected val logger = Logger()

  trait Execable {
    def execCallbacks(): Unit
  }

  class CallbackHandler extends Execable {
    @volatile private var functions = new collection.mutable.ArrayBuffer[() => Unit]()
    def registerCallback(f: => Unit): Unit = {
      functions += (() => f)
    }
    def execCallbacks(): Unit = functions.foreach(f => f())
  }

  trait PassableCallbackHandler[A] extends Execable {
    def unit: A

    @volatile private var functions = new collection.mutable.ArrayBuffer[A => A]() 
    def registerCallback(f: A => A): Unit = {
      functions += f
    }
    def execCallbacks(): Unit = functions.foldLeft(unit) { case (cur, f) => f(cur) }
  }

  private val createHandler = new CallbackHandler
  private val createFinishedHandler = new CallbackHandler
  private val openHandler = new PassableCallbackHandler[Boolean] { val unit = false }
  private val closeHandler = new CallbackHandler
  private val deleteHandler = new CallbackHandler 

  protected def onCreate(f: => Unit): Unit = createHandler.registerCallback(f)
  protected def onCreateFinished(f: => Unit): Unit = createFinishedHandler.registerCallback(f)
  protected def onOpen(f: Boolean => Boolean): Unit = openHandler.registerCallback(f)
  protected def onClose(f: => Unit): Unit = closeHandler.registerCallback(f)
  protected def onDelete(f: => Unit): Unit = deleteHandler.registerCallback(f)

  // TODO: what are the semantics for each? they should be specified clearly

  def create(): Unit = {createHandler.execCallbacks(); createFinishedHandler.execCallbacks()}
  def open(): Unit = openHandler.execCallbacks()
  def close(): Unit = closeHandler.execCallbacks() 
  def delete(): Unit = deleteHandler.execCallbacks()

  // Default timeout values.
  val timeoutZooKeeper = 5 * 1000
  val timeoutCreatePartition = 15 * 1000

  override def toString = protToString
  protected def protToString: String = 
    "<Namespace: %s>".format(name)
}
