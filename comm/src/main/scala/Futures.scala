package edu.berkeley.cs.scads.comm

import scala.actors._
import scala.concurrent.SyncVar
import net.lag.logging.Logger

import org.apache.avro.generic.IndexedRecord
import java.util.concurrent.{ LinkedBlockingQueue, TimeUnit }
import java.util.{LinkedList, Queue}
import java.lang.ref.WeakReference
import java.lang.RuntimeException

/**
 * This is the base trait for any type of future in SCADS.
 */
trait ScadsFuture[+T] { self =>

  /**
   * Cancels the current future
   */
  def cancel(): Unit

  /**
   * Block on the future until T is ready
   */
  def get(): T

  /**
   * Same as get()
   */
  def apply(): T = get()

  /**
   * Block for up to timeout.
   */
  def get(timeout: Long, unit: TimeUnit = TimeUnit.MILLISECONDS): Option[T]

  /**
   * True iff the future has already been set
   */
  def isSet: Boolean

  /**
   * Return a new future which is backed by this future and maps the
   * result according to the given function
   */
  def map[T1](f: T => T1): ScadsFuture[T1] = new ScadsFuture[T1] {
    def cancel = self.cancel
    def get = f(self.get)
    def get(timeout: Long, unit: TimeUnit = TimeUnit.MILLISECONDS) =
      self.get(timeout, unit) map f 
    def isSet = self.isSet
  }

}

object FutureReference {
  val logger = Logger()

  val staleMessages = new java.lang.ref.ReferenceQueue[MessageFuture[_]]()
  val cleanupThread = new Thread("Failed Message Cleanup") {
    override def run(): Unit = {
      while(true) {
        val futureRef = staleMessages.remove.asInstanceOf[FutureReference[_]]
        if(futureRef.unregister)
          logger.warning("Removing unsatisfied gced future from message regsistry: %s %s %s", futureRef.remoteService, futureRef.dest,futureRef.request)
      }
    }
  }
  cleanupThread.setDaemon(true)
  cleanupThread.start()
}

class FutureReference[MessageType <: IndexedRecord](future: MessageFuture[MessageType])(implicit val handler: ServiceRegistry[MessageType]) extends java.lang.ref.WeakReference(future, FutureReference.staleMessages) with MessageReceiver[MessageType] {
  val remoteService = handler.registerService(this)
  val request = future.request
  val dest = future.dest
  MessageFuture.logger.debug("Future registered: %s %s to %s", remoteService, request, dest)
  private var registered = true

  def receiveMessage(src: Option[RemoteServiceProxy[MessageType]], msg: MessageType): Unit = synchronized {
    MessageFuture.logger.debug("Attempting delivery to %s", remoteService)
    val future = get().asInstanceOf[MessageFuture[MessageType]]
    if(!unregister) FutureReference.logger.warning("GCed future %s", remoteService)
    clear()
    if(future != null)
      future.receiveMessage(src, msg)
    else
      FutureReference.logger.debug("Message for garbage collected future received: %s %s from %s", remoteService, msg, src)
  }

  def unregister = synchronized {
    if(registered) {
      registered = false
      handler.unregisterService(remoteService)
      true
    } else false
  }
}

object MessageFuture {
  val logger = Logger()
  implicit def toFutureCollection[MessageType <: IndexedRecord](futures: Seq[MessageFuture[MessageType]]) = new FutureCollection[MessageType](futures)
}

case class MessageFuture[MessageType <: IndexedRecord](val dest: RemoteServiceProxy[MessageType], val request: MessageType)(implicit val registry: ServiceRegistry[MessageType]) extends Future[MessageType] {
  protected[comm] val sender = new SyncVar[Option[RemoteServiceProxy[MessageType]]]
  protected val message = new SyncVar[MessageType]
  protected var forwardList: List[Queue[MessageFuture[MessageType]]] = List()

  val remoteService = (new FutureReference(this)).remoteService

  /* Note: doesn't really implement interface correctly */
  def inputChannel = new InputChannel[MessageType] {
    def ?(): MessageType = message.get
    def reactWithin(msec: Long)(pf: PartialFunction[Any, Unit]): Nothing = throw new RuntimeException("Unimplemented")
    def react(f: PartialFunction[MessageType, Unit]): Nothing = throw new RuntimeException("Unimplemented")
    def receive[R](f: PartialFunction[MessageType, R]): R = f(message.get)
    def receiveWithin[R](msec: Long)(f: PartialFunction[Any, R]): R = f(message.get(msec).getOrElse(throw new RuntimeException("timeout")))
  }

  def apply() = message.get

  //The actual sender of a message
  def source =  sender.get

  protected var respondFunctions: List[MessageType => Unit] = Nil
  def respond(r: MessageType => Unit): Unit = synchronized {
    if(message.isSet)
      r(message.get)
    else
      respondFunctions ::= r
  }

  def get(): MessageType = message.get

  def get(timeout: Long): Option[MessageType] =
    get(timeout, TimeUnit.MILLISECONDS)

  def get(timeout: Long, unit: TimeUnit): Option[MessageType] =
    message.get(unit.toMillis(timeout))

  def isSet: Boolean = message.isSet

  /**
   * Either forward the result to the following queue, or request that it be forwarded upon arrival.
   */
  def forward(dest: Queue[MessageFuture[MessageType]]): Unit = synchronized {
    if(message.isSet)
      dest.offer(this)
    else
      forwardList ::= dest
  }

  def receiveMessage(src: Option[RemoteServiceProxy[MessageType]], msg: MessageType): Unit = synchronized {
    message.set(msg)
    sender.set(src)
    forwardList.foreach(_.offer(this))
    respondFunctions.foreach(_(msg))
  }
}

class FutureCollection[MessageType <: IndexedRecord](val futures: Seq[MessageFuture[MessageType]]) {
  val responses = new java.util.concurrent.LinkedBlockingQueue[MessageFuture[MessageType]]
  futures.foreach(_.forward(responses))

  def blockFor(count: Int): Seq[MessageFuture[MessageType]] = (1 to count).map(_ => responses.take())

  def blockFor(count: Int, timeout: Long, unit: TimeUnit): Seq[MessageFuture[MessageType]] = {
    (1 to count).map(_ => Option(responses.poll(timeout, unit)).getOrElse(throw new RuntimeException("TIMEOUT")))
  }
}

class FutureTimeoutException extends RuntimeException
class FutureException(ex: Throwable) extends RuntimeException(ex)

/**
 * Default synchronized-based implementation of ScadsFuture
 */
class BlockingFuture[T] extends ScadsFuture[T] {
  private var ex: Throwable = _
  private var done          = false
  private var elem: T       = _

  def cancel() { sys.error("UNIMPLEMENTED") }

  def isSet: Boolean = synchronized { done }

  def await(timeout: Long): T = {
    val opt = get(timeout, TimeUnit.MILLISECONDS)
    opt.getOrElse(throw new FutureTimeoutException)
  }

  def await(): T = await(0L)

  def get(): T = await(0L)

  def get(timeout: Long, unit: TimeUnit): Option[T] = {
    synchronized {
      if (!done)
        wait(unit.toMillis(timeout))
      if (!done) None
      else if (ex ne null)
        throw new FutureException(ex)
      else {
        assert(elem.asInstanceOf[AnyRef] ne null, "Element cannot be null if done")
        Some(elem)
      }
    }
  }

  def finish(e: T) {
    require(e.asInstanceOf[AnyRef] ne null)
    synchronized {
      if (done)
        throw new IllegalStateException("Future already done")
      done = true
      elem = e
      notifyAll()
    }
  }

  def finishWithError(error: Throwable) {
    require(error ne null)
    synchronized {
      if (done)
        throw new IllegalStateException("Future already done")
      done = true
      ex   = error
      notifyAll()
    }
  }
}

/**
 *  Default future which wraps a computation in a future. This computation is allowed to fail. 
 *  The semantics are that if the computation fails (by throwing an exception), then calls to get() will also throw an
 *  exception. Otherwise, the value from the computation will be returned.
 *  Note that the computation does NOT start running until the first call to
 *  get() is invoked
 */
trait ComputationFuture[T] extends ScadsFuture[T] {

  import java.util.concurrent.atomic.AtomicBoolean
  import scala.concurrent.SyncVar

  /**
   * The computation that should run with this future. Is guaranteed to only
   * be called at most once. Timeout hint is a hint for how long the user is
   * willing to wait for this computation to compute
   */
  protected def compute(timeoutHint: Long, unit: TimeUnit): T 

  /**
   * Signals a request to cancel the computation. Is guaranteed to only be
   * called at most once.
   */
  protected def cancelComputation: Unit

  /** Guard the computation from only running once */
  private val computeGuard = new AtomicBoolean(false)

  /** Guard the cancellation request from only being invoked once */
  private val cancelGuard = new AtomicBoolean(false)

  /** Store the result */
  private val syncVar = new SyncVar[Either[T, Throwable]]

  def cancel = 
    if (cancelGuard.compareAndSet(false, true)) 
      cancelComputation

  def get = get(java.lang.Long.MAX_VALUE, TimeUnit.MILLISECONDS).get 

  @inline private def dispatchResult(res: Either[T, Throwable]): T = res match {
    case Left(value) => value
    case Right(ex)   => throw new RuntimeException(ex)
  }

  def get(timeout: Long, unit: TimeUnit) = { 
    if (computeGuard.compareAndSet(false, true)) {
      // this is a bit of a hack now, since the computation blocks the current
      // thread. will have to move to a separate thread for true get semantics
      try syncVar.set(Left(compute(timeout, unit))) catch {
        case e: Throwable => syncVar.set(Right(e))
      }
      Some(dispatchResult(syncVar.get))
    } else {
      // other thread beat us out to finishing the get handler- in this
      // case just wait on the sync var
      syncVar.get(unit.toMillis(timeout)).map(dispatchResult)
    }   
  }   
  def isSet = syncVar.isSet
}
