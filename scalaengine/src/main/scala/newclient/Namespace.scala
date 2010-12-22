package edu.berkeley.cs.scads.storage.newclient

trait Namespace {
  def name: String

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

  protected def onCreate(f: => Unit): Unit = createHandler.registerCallback(f) 
  protected def onOpen(f: => Unit): Unit = openHandler.registerCallback(f)
  protected def onClose(f: => Unit): Unit = closeHandler.registerCallback(f)
  protected def onDelete(f: => Unit): Unit = deleteHandler.registerCallback(f)

  def create(): Unit = createHandler.execCallbacks()
  def open(): Unit = openHandler.execCallbacks()
  def close(): Unit = closeHandler.execCallbacks() 
  def delete(): Unit = deleteHandler.execCallbacks()
}
