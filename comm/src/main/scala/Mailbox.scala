package edu.berkeley.cs.scads.comm

import java.util.concurrent.{ArrayBlockingQueue, PriorityBlockingQueue, ThreadPoolExecutor, CountDownLatch, TimeUnit}
import org.apache.avro.generic.IndexedRecord
import collection.mutable.ArrayBuffer
import java.util.{PriorityQueue, Queue}
import util.Sorting._

/**
 * Priority:
 * 0 => Highest
 * inf => Lowest
 */

trait Mailbox[MessageType <: IndexedRecord] {
  def add(msg : Envelope[MessageType])  : Boolean

  @volatile var keepMsgInMailbox = false

  def add(src : Option[RemoteService[MessageType]],  msg: MessageType) : Boolean = {
    this.add(Envelope(src, msg))
  }

  def addFirst(msg : Envelope[MessageType]) : Unit

  def peek : Envelope[MessageType]

  def poll : Envelope[MessageType]

  def hasNext : Boolean

  def next() : Envelope[MessageType]

  def remove() : Unit

  def reset() : Unit

  def isEmpty  : Boolean

  def drainTo(c : Mailbox[MessageType]) = {
    addAll(this)
    clear()
  }

  def clear() : Unit

  def addAll(c : Mailbox[MessageType]) : Unit

  def size() : Int

  def apply(fn : PartialFunction[Envelope[MessageType], Unit]) = {
    reset()
    while(hasNext){
      keepMsgInMailbox = false
      fn(next())
      if (!keepMsgInMailbox) {
        remove()
      }
    }
  }
}

class PriorityBlockingMailbox[MessageType <: IndexedRecord](prioFn : MessageType => Int)
  extends  Mailbox[MessageType]{

  var queue = new PriorityBlockingQueue[Envelope[MessageType]](5, PriorityGenerator(prioFn))

  var iter = queue.iterator

  override def size() : Int = queue.size()

  override def clear() : Unit = queue.clear()

  override def peek : Envelope[MessageType] = queue.peek()

  override def poll : Envelope[MessageType] = queue.poll()

  override def add(msg : Envelope[MessageType]) : Boolean  = queue.add(msg)

  override def hasNext : Boolean  = iter.hasNext

  override def next() : Envelope[MessageType]  = iter.next()

  override def remove() : Unit = iter.remove


  override def reset() = iter = queue.iterator

  override def addAll(c : Mailbox[MessageType])  = throw new RuntimeException("Not Implemented")

  override def addFirst(msg : Envelope[MessageType]) = throw new RuntimeException("Not Implemented")

  override def isEmpty = queue.peek() == null
}


class PlainMailbox[MessageType <: IndexedRecord]()
  extends ArrayBuffer[Envelope[MessageType]](5) with Mailbox[MessageType]  {

  private var pos : Int = -1
  private var nextReset : Boolean = false

  /**
   * No motification to the mailbox are allowed during the apply
   * except addPrio and add
   */
  override def apply(fn : PartialFunction[Envelope[MessageType], Unit]) = {
    reset()
    while(hasNext){
      keepMsgInMailbox = false
      fn(next())
      if (!keepMsgInMailbox) {
        remove()
      }
    }
  }

  override def addAll(c : Mailbox[MessageType]) : Unit   = {
    ++=(c.asInstanceOf[PlainMailbox[MessageType]])
  }

  override def addFirst(msg : Envelope[MessageType]) = {
   +=:(msg)
   pos += 1
   nextReset = true
  }

  override def add(msg : Envelope[MessageType]) = {
    append(msg)
    true
  }

  def poll() = {
    if(size0 == 0) null
    else remove(0)
  }

  def peek = {
   if(size0 == 0) null
   else apply(0)
  }

  override def hasNext: Boolean = {
    if(nextReset){
      reset()
    }
    pos + 1< length
  }

  override def next: Envelope[MessageType] = {
    pos += 1
    apply(pos)
  }

  override def remove(): Unit = {
    remove(pos)
    pos -= 1
  }

  override def reset() = {
    pos = -1
    nextReset = false
  }

  /**
   * Compare should return true iff its first parameter is strictly less than its second parameter.
   */
  @inline private def less(prioFn : MessageType => Int, m1 : AnyRef, m2 : AnyRef) = {
    prioFn(m1.asInstanceOf[Envelope[MessageType]].msg) < prioFn(m1.asInstanceOf[Envelope[MessageType]].msg)
  }


  def sort(prioFn : MessageType => Int) = {
    stableSort(prioFn, array, 0, size0 - 1, new Array[AnyRef](size0 ))
  }

  private def stableSort(prioFn : MessageType => Int, a: Array[AnyRef], lo: Int, hi: Int, scratch: Array[AnyRef])  {
    if (lo < hi) {
      val mid = (lo+hi) / 2
      stableSort(prioFn, a, lo, mid, scratch)
      stableSort(prioFn, a, mid+1, hi, scratch)
      var k, t_lo = lo
      var t_hi = mid + 1
      while (k <= hi) {
        if ((t_lo <= mid) && ((t_hi > hi) || (!less(prioFn, a(t_hi), a(t_lo))))) {
          scratch(k) = a(t_lo)
          t_lo += 1
        } else {
          scratch(k) = a(t_hi)
          t_hi += 1
        }
        k += 1
      }
      k = lo
      while (k <= hi) {
        a(k) = scratch(k)
        k += 1
      }
    }
  }



}

object PriorityGenerator {
  /**
   * Creates a PriorityGenerator that uses the supplied function as priority generator
   */
  def apply[MessageType <: IndexedRecord](priorityFunction: MessageType => Int): PriorityGenerator[MessageType] = new PriorityGenerator[MessageType] {
    def gen(message: MessageType): Int = priorityFunction(message)
  }

}

/**
 * A PriorityGenerator is a convenience API to create a Comparator that orders the messages of a
 * PriorityDispatcher
 */
abstract class PriorityGenerator[MessageType <: IndexedRecord] extends java.util.Comparator[Envelope[MessageType]] {
  def gen(message: MessageType): Int

  final def compare(thisMessage: Envelope[MessageType], thatMessage: Envelope[MessageType]): Int =
    gen(thisMessage.msg) - gen(thatMessage.msg)
}
