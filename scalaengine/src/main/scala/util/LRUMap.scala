package edu.berkeley.cs.scads.util

private[util] trait LinkedListElem[T1, T2] {
  private[util] var _prev: LinkedListElem[T1, T2] = null
  private[util] var _next: LinkedListElem[T1, T2] = null
  private[util] def value1: T1
  private[util] var value2: T2 = _


  private[util] def remove {
    _prev._next = _next
    _next._prev = _prev
  }

  private[util] def addAtHead(what: LinkedListElem[T1, T2]) {
    what._next = _next
    what._prev = this
    _next._prev = what
    this._next = what
    what
  }

  private[util] def addAtTail(what: LinkedListElem[T1, T2]) {
    what._prev = _prev
    what._next = this
    _prev._next = what
    this._prev = what
    what
  }
}



/**
 *
 */
class LRUMap[K, V](initMaxSize: Int, loadFactor: Option[Float], expiredFunc: ((K, V) => Unit)*) extends LinkedListElem[K, V] {
  import java.util.HashMap

  def this(size: Int) = this(size, None)

  private var _maxSize = initMaxSize

  def maxSize = _maxSize

  def updateMaxSize(newMaxSize: Int) {
    val oldMaxSize = _maxSize
    _maxSize = newMaxSize

    if (newMaxSize < oldMaxSize) {
      doRemoveIfTooMany()
    }
  }

  _prev = this
  _next = this

  private[util] def value1: K = throw new NullPointerException("Foo")


  private[this] val localMap = new HashMap[K, LinkedListElem[K, V]](maxSize / 4, loadFactor.getOrElse(0.75f))

  def get(key: K): Option[V] = localMap.get(key) match {
    case null => None
    case v =>
      v.remove
    addAtHead(v)
    Some(v.value2)
  }

  def apply(key: K) = get(key).get

  def contains(key: K): Boolean = localMap.containsKey(key)

  def -(key: K) = remove(key)

  def remove(key: K) {
    localMap.get(key) match {
      case null =>
	case v =>
          v.remove
      localMap.remove(key)
    }
  }

  def update(key: K, value: V) {
    localMap.get(key) match {
      case null =>
        val what = new LinkedListElem[K, V] {def value1 = key}
      what.value2 = value
      addAtHead(what)
      localMap.put(key, what)

      doRemoveIfTooMany()

      case v =>
        v.remove
      addAtHead(v)
      v.value2 = value
    }
  }

  /**
   * Override this method to implement a test to see if a particular
   * element can be expired from the cache
   */
  protected def canExpire(k: K, v: V): Boolean = {
    true
  }

  /**
   * A mechanism for expiring elements from cache.  This method
   * can devolve into O(n ^ 2) if lots of elements can't be
   * expired
   */
  private def doRemoveIfTooMany() {
    while (localMap.size > maxSize) {
      var toRemove = _prev
      while (!canExpire(toRemove.value1, toRemove.value2)) {
        toRemove = toRemove._prev
        if (toRemove eq this) return
      }
      toRemove.remove
      localMap.remove(toRemove.value1)
      expired(toRemove.value1, toRemove.value2)
      expiredFunc.foreach(_(toRemove.value1, toRemove.value2))
    }
  }

  /**
   * Called when a key/value pair is removed
   */
  protected def expired(key: K, value: V) {

  }

  def keys: List[K] = elements.toList.map(_._1)

  def elements: Iterator[(K, V)] = {
    val set = localMap.entrySet.iterator
    new Iterator[(K, V)] {
      def hasNext = set.hasNext
      def next: (K, V) = {
        val k = set.next
        (k.getKey, k.getValue.value2)
      }
    }
  }

  def size: Int = localMap.size

}



        
