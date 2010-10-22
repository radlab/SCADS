package edu.berkeley.cs.scads.comm

import scala.actors._
import scala.concurrent.SyncVar
import net.lag.logging.Logger

import java.util.concurrent.{ LinkedBlockingQueue, TimeUnit }
import java.util.{LinkedList, Queue}

/**
 * This is the base trait for any type of future in SCADS.
 */
trait ScadsFuture[T] {

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

}

object MessageFuture {
  implicit def toFutureCollection(futures: Seq[MessageFuture]): FutureCollection = new FutureCollection(futures)
}

class BatchFuture(val pos : Int) extends MessageFuture{

  protected var forwardList: List[Queue[MessageFuture]] = List()

  var future : MessageFuture = null;
    /**
   * Cancels the current future
   */
  def cancel(): Unit = future.cancel

  /**
   * Block on the future until T is ready
   */
  def get(): MessageBody = {
    assert(future != null)
    future.get() match {
      case BatchRequest(ranges) => return ranges(pos)
      case a : MessageBody => return a
      case _ => throw new RuntimeException("Unexpected message") // TODO better failure handling
    }
  }

  /**
   * Block for up to timeout.
   */
  def get(timeout: Long, unit: TimeUnit = TimeUnit.MILLISECONDS): Option[MessageBody]  = {
    assert(future != null)
    future.get(timeout, unit) match {
      case Some(BatchRequest(ranges)) => Some(ranges(pos))
      case a => a
    }
  }

  /**
   * True iff the future has already been set
   */
  def isSet: Boolean = future.isSet
                  
  class DirectForwardQueue extends LinkedList[MessageFuture]{
    override def add(e : MessageFuture) = offer(e)
    override def offer(e : MessageFuture) :  Boolean = {
      offerRequest()
      return true;
    }
    override def remove() : MessageFuture = throw new RuntimeException("Not implemented")
    override def poll() : MessageFuture   = throw new RuntimeException("Not implemented")
    override def element() : MessageFuture = throw new RuntimeException("Not implemented")
    override def peek() : MessageFuture = throw new RuntimeException("Not implemented")
  }

  private def offerRequest() : Unit = {
    forwardList.foreach(_.offer(this))
  }

  def forward(dest: Queue[MessageFuture]): Unit = {
    if(isSet)
      dest.offer(this)
    else{
      forwardList ::= dest
      future.forward(new DirectForwardQueue())      
    }
  }

  def receiveMessage(src: Option[RemoteActorProxy], msg: MessageBody): Unit  = {
    throw new RuntimeException("Not implemented")
  }

  def source : Option[RemoteActorProxy] = future.source
  def respond(r: (MessageBody) => Unit): Unit  = future.respond(r)
}

trait MessageFuture extends ScadsFuture[MessageBody] {

  def forward(dest: Queue[MessageFuture]): Unit

  def receiveMessage(src: Option[RemoteActorProxy], msg: MessageBody): Unit


  def source : Option[RemoteActorProxy]
  def respond(r: (MessageBody) => Unit): Unit


}//TODO Rework message hierarchy

class MessageFutureImpl extends Future[MessageBody] with MessageFuture with MessageReceiver {
  protected[comm] val remoteActor = MessageHandler.registerService(this)
  protected val sender = new SyncVar[Option[RemoteActorProxy]]
  protected val message = new SyncVar[MessageBody]
  protected var forwardList: List[Queue[MessageFuture]] = List()

  /* Note: doesn't really implement interface correctly */
  def inputChannel = new InputChannel[MessageBody] {
    def ?(): MessageBody = message.get
    def reactWithin(msec: Long)(pf: PartialFunction[Any, Unit]): Nothing = throw new RuntimeException("Unimplemented")
    def react(f: PartialFunction[MessageBody, Unit]): Nothing = throw new RuntimeException("Unimplemented")
    def receive[R](f: PartialFunction[MessageBody, R]): R = f(message.get)
    def receiveWithin[R](msec: Long)(f: PartialFunction[Any, R]): R = f(message.get(msec).getOrElse(ProcessingException("timeout", "")))
  }

  def source =  sender.get
  def respond(r: (MessageBody) => Unit): Unit = r(message.get)

  def get(): MessageBody = message.get

  def get(timeout: Int): Option[MessageBody] =
    get(timeout, TimeUnit.MILLISECONDS)

  def get(timeout: Long, unit: TimeUnit): Option[MessageBody] = 
    message.get(unit.toMillis(timeout))

  def isSet: Boolean = message.isSet

  /**
   * Cancel this request by unregistering with the message handler.
   * Note no action is taken to cancel processing initiating message server-side.
   */
  def cancel: Unit = MessageHandler.unregisterActor(remoteActor)

  /**
   * Either forward the result to the following queue, or request that it be forwarded upon arrival.
   */
  def forward(dest: Queue[MessageFuture]): Unit = synchronized {
    if(message.isSet)
      dest.offer(this)
    else
      forwardList ::= dest
  }



  def receiveMessage(src: Option[RemoteActorProxy], msg: MessageBody): Unit = synchronized {
    MessageHandler.unregisterActor(remoteActor)
    message.set(msg)
    sender.set(src)
    forwardList.foreach(_.offer(this))
  }
}

class FutureCollection(val futures: Seq[MessageFuture]) {
  val responses = new java.util.concurrent.LinkedBlockingQueue[MessageFuture]
  futures.foreach(_.forward(responses))

  def blockFor(count: Int): Seq[MessageFuture] = (1 to count).map(_ => responses.take())
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

  def cancel() { error("UNIMPLEMENTED") }

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
