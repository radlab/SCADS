package edu.berkeley.cs.scads
package storage

object Slice {
  def apply[A <% Ordered[A]](start: Option[A], end: Option[A]): Slice[A] = start match {
    case Some(startKey) => end match {
      case Some(endKey) => BoundedSlice(startKey, endKey)
      case None => ToRight(startKey)
    }
    case None => end match {
      case Some(endKey) => FromLeft(endKey)
      case None => EntireRange()
    }
  }
}

sealed abstract class Slice[A <% Ordered[A]] {
  /**
   * Removes slice out of this slice. Returns a new slice which result
   * from this operation
   */
  def remove(slice: Slice[A]): Slice[A]

  def foreach[U](f: (Option[A], Option[A]) => U): Unit

  protected def max(a: A, b: A) = if (a < b) b else a
  protected def min(a: A, b: A) = if (a > b) b else a
}

/**
 * Not an object so we can capture implicit evidence for view bound on A
 */
case class EntireRange[A <% Ordered[A]]() extends Slice[A] {
  def remove(slice: Slice[A]) = slice match {
    case EntireRange() => EmptyRange()
    case EmptyRange()  => this
    case FromLeft(endKey) => ToRight(endKey)
    case ToRight(startKey) => FromLeft(startKey)
    case BoundedSlice(startKey, endKey) =>
      CompositeSlice(List(FromLeft(startKey), ToRight(endKey)))
    case CompositeSlice(_) =>
      throw new UnsupportedOperationException("Cannot remove composite for now")
  }

  def foreach[U](f: (Option[A], Option[A]) => U) { f(None, None) }
}

/**
 * Not an object so we can capture implicit evidence for view bound on A
 */
case class EmptyRange[A <% Ordered[A]]() extends Slice[A] {
  /** 
   * Remove from the EmptyRange is still the EmptyRange
   */
  def remove(slice: Slice[A]) = this
  def foreach[U](f: (Option[A], Option[A]) => U) {}
}

sealed abstract class CustomSlice[A <% Ordered[A]] extends Slice[A]

case class FromLeft[A <% Ordered[A]](endKey: A) extends CustomSlice[A] {

  def remove(slice: Slice[A]) = slice match {
    case EntireRange() => EmptyRange()
    case EmptyRange()  => this
    case FromLeft(thatEndKey) if endKey.compare(thatEndKey) == 0 => EmptyRange()
    case FromLeft(thatEndKey) =>
      BoundedSlice(min(endKey, thatEndKey), max(endKey, thatEndKey))
    case ToRight(startKey) if startKey >= endKey => this
    case ToRight(startKey) => FromLeft(startKey)
    case BoundedSlice(thatStartKey, thatEndKey) if thatStartKey >= endKey => this
    case BoundedSlice(thatStartKey, thatEndKey) if thatEndKey >= endKey => FromLeft(thatStartKey)
    case BoundedSlice(thatStartKey, thatEndKey) =>
      CompositeSlice(List(FromLeft(thatStartKey), BoundedSlice(thatEndKey, endKey)))
    case CompositeSlice(_) =>
      throw new UnsupportedOperationException("Cannot remove composite for now")
  }

  def foreach[U](f: (Option[A], Option[A]) => U) { f(None, Some(endKey)) }
}

case class ToRight[A <% Ordered[A]](startKey: A) extends CustomSlice[A] {

  def remove(slice: Slice[A]) = slice match {
    case EntireRange() => EmptyRange()
    case EmptyRange()  => this
    case FromLeft(thatEndKey) if startKey >= thatEndKey => this
    case FromLeft(thatEndKey) => ToRight(thatEndKey)
    case ToRight(thatStartKey) if startKey.compare(thatStartKey) == 0 => EmptyRange()
    case ToRight(thatStartKey) =>
      BoundedSlice(min(startKey, thatStartKey), max(startKey, thatStartKey))
    case BoundedSlice(thatStartKey, thatEndKey) if thatEndKey <= startKey => this
    case BoundedSlice(thatStartKey, thatEndKey) if thatStartKey <= startKey => ToRight(thatEndKey)
    case BoundedSlice(thatStartKey, thatEndKey) =>
      CompositeSlice(List(BoundedSlice(startKey, thatStartKey), ToRight(thatEndKey)))
    case CompositeSlice(_) =>
      throw new UnsupportedOperationException("Cannot remove composite for now")
  }

  def foreach[U](f: (Option[A], Option[A]) => U) { f(Some(startKey), None) }
}

case class BoundedSlice[A <% Ordered[A]](startKey: A, endKey: A) extends CustomSlice[A] {
  assert(endKey > startKey)                                    

  def remove(slice: Slice[A]) = slice match {
    case EntireRange() => EmptyRange()
    case EmptyRange()  => this
    case FromLeft(thatEndKey) if thatEndKey <= startKey => this
    case FromLeft(thatEndKey) if thatEndKey < endKey => BoundedSlice(thatEndKey, endKey)
    case FromLeft(thatEndKey) => EmptyRange()
    case ToRight(thatStartKey) if thatStartKey >= endKey => this
    case ToRight(thatStartKey) if thatStartKey > startKey => BoundedSlice(startKey, thatStartKey)
    case ToRight(thatStartKey) => EmptyRange()
    case BoundedSlice(thatStartKey, thatEndKey) if thatStartKey.compare(startKey) == 0 && thatEndKey.compare(endKey) == 0 => EmptyRange()
    case BoundedSlice(thatStartKey, thatEndKey) if endKey <= thatStartKey || thatEndKey <= startKey => this
    case BoundedSlice(thatStartKey, thatEndKey) if thatStartKey <= startKey =>
      if (thatEndKey < endKey) BoundedSlice(thatEndKey, endKey)
      else EmptyRange()
    case BoundedSlice(thatStartKey, thatEndKey) =>
      if (thatEndKey < endKey) CompositeSlice(List(BoundedSlice(startKey, thatStartKey), BoundedSlice(thatEndKey, endKey)))
      else BoundedSlice(startKey, thatStartKey)
    case CompositeSlice(_) =>
      throw new UnsupportedOperationException("Cannot remove composite for now")
  }

  def foreach[U](f: (Option[A], Option[A]) => U) { f(Some(startKey), Some(endKey)) }
}

case class CompositeSlice[A <% Ordered[A]](slices: List[Slice[A]]) extends CustomSlice[A] {
  // TODO: assert that all slices are in order and do not overlap! 

  def remove(slice: Slice[A]) = slice match {
    case CompositeSlice(_) =>
      throw new UnsupportedOperationException("Cannot remove composite for now")
    case _ =>
      slices.flatMap(_.remove(slice) match {
        case CompositeSlice(xs) => xs
        case EmptyRange() => Nil
        case x => List(x)
      }) match {
        case Nil => EmptyRange()
        case x :: Nil => x
        case xs => CompositeSlice(xs)
      }
  }

  def foreach[U](f: (Option[A], Option[A]) => U) { 
    slices.foreach(_.foreach(f))
  }
}
