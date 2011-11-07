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

  def peek : Envelope[MessageType]

  def poll : Envelope[MessageType]

  def iter : java.util.Iterator[Envelope[MessageType]]

  def isEmpty  : Boolean

  def size() : Int

  def pollAll(f: Envelope[MessageType] => Boolean) : Seq[Envelope[MessageType]] = {
    val it = iter
    var buffer = new collection.mutable.ArrayBuffer[Envelope[MessageType]]()
    while(it.hasNext){
      val item = it.next()
      if (f(item)){
        buffer += item
        it.remove()
      }
    }
    buffer
  }

  def apply(fn : PartialFunction[Envelope[MessageType], Unit]) = {
    var it = iter
    while(it.hasNext){
      keepMsgInMailbox = false
      fn(it.next())
      if (!keepMsgInMailbox) {
        it.remove()
      }
    }
  }
}

class PriorityBlockingMailbox[MessageType <: IndexedRecord](prioFn : MessageType => Int)
  extends PriorityBlockingQueue[Envelope[MessageType]](5, PriorityGenerator(prioFn)) with Mailbox[MessageType]{

  def iter = iterator

  override def isEmpty = peek() == null
}


class PlainMailbox[MessageType <: IndexedRecord](prioFn : MessageType => Int)
  extends ArrayBuffer[Envelope[MessageType]](5) with Mailbox[MessageType]  {

  def iter = new  ArrayIter(this)

  def add(msg : Envelope[MessageType]) = {
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

  class ArrayIter (var buffer : ArrayBuffer[Envelope[MessageType]]) extends java.util.Iterator[Envelope[MessageType]] {
    var pos = -1
    def hasNext: Boolean = pos + 1< buffer.length

    def next: Envelope[MessageType] = {
      pos += 1
      buffer(pos)
    }
    def remove: Unit = {
      buffer.remove(pos)
      pos -= 1
    }
  }

  def drainTo(c : PlainMailbox[MessageType]) = {
    c.++=(this)
    clear()
  }

  /**
   * Compare should return true iff its first parameter is strictly less than its second parameter.
   */
  @inline private def less(m1 : AnyRef, m2 : AnyRef) = {
    prioFn(m1.asInstanceOf[Envelope[MessageType]].msg) < prioFn(m1.asInstanceOf[Envelope[MessageType]].msg)
  }


  def sort() = {
    stableSort(array, 0, size0 - 1, new Array[AnyRef](size0 ))
  }

  private def stableSort(a: Array[AnyRef], lo: Int, hi: Int, scratch: Array[AnyRef])  {
    if (lo < hi) {
      val mid = (lo+hi) / 2
      stableSort(a, lo, mid, scratch)
      stableSort(a, mid+1, hi, scratch)
      var k, t_lo = lo
      var t_hi = mid + 1
      while (k <= hi) {
        if ((t_lo <= mid) && ((t_hi > hi) || (!less(a(t_hi), a(t_lo))))) {
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
