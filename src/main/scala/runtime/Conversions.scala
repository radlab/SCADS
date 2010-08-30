package com.googlecode.avro
package runtime

import org.apache.avro.Schema
import Schema.Type
import org.apache.avro.generic.{ GenericArray, GenericData }
import org.apache.avro.util.Utf8

import java.nio.ByteBuffer

import scala.collection.Traversable
import scala.collection.mutable.ArrayBuffer

import java.util.{ Map => JMap, Iterator => JIterator, TreeMap }

/**
 * http://stackoverflow.com/questions/2257341/java-scala-deep-collections-interoperability
 */
trait ==>>[A, B] extends (A => B) {
  def apply(a: A): B
}

object IdentityTransform extends (Any ==>> Any) {
  def apply(a: Any) = a
}

/**
 * Wraps existing GenericArray, which is not associated with a Schema, with a
 * Schema instance
 */
class GenericArrayWrapper[E](schema: Schema, impl: GenericArray[E]) extends GenericArray[E] {
  require(schema ne null, "Schema cannot be null")
  require(schema.getType == Type.ARRAY, "Schema must be ARRAY type")
  require(impl ne null, "Impl GenericArray cannot be null")

  @inline private def isElemArray =
    schema.getElementType.getType == Type.ARRAY

  @inline private def wrapInnerArray(e: E): E =
    if (e.asInstanceOf[AnyRef] eq null) null.asInstanceOf[E]
    else {
      val rtn = new GenericArrayWrapper[Any](schema.getElementType, e.asInstanceOf[GenericArray[Any]])
      rtn.asInstanceOf[E]
    }

  def add(elem: E) = impl.add(elem)
  def clear = impl.clear
  def peek = 
    if (isElemArray) {
      val e = impl.peek
      assert(e.isInstanceOf[GenericArray[_]], "Element is not array, even though Schema indicates so")
      wrapInnerArray(e)
    } else impl.peek
  def size = impl.size
  def iterator =
    if (isElemArray) {
      val iter = impl.iterator
      new JIterator[E] {
        def hasNext = iter.hasNext
        def next = {
          val e = iter.next
          assert(e.isInstanceOf[GenericArray[_]], "Element is not array, even though Schema indicates so")
          wrapInnerArray(e)
        }
        def remove = iter.remove 
      }
    } else impl.iterator
  def getSchema = schema
}

trait BuilderFactory[Elem, Coll] {
  def newBuilder: scala.collection.mutable.Builder[Elem, Coll]
}

trait HasBuilderFactories extends HasCollectionBuilders
                          with    HasImmutableBuilders
                          with    HasMutableBuilders

trait HasAvroConversions extends HasAvroPrimitiveConversions 
                         with    HasBuilderFactories 
                         with    HasTraversableConversions
                         with    HasOptionConversions {


  def convert[A, B](a: A)(implicit trfm: A ==>> B): B = a

  implicit def identity[A] =
    IdentityTransform.asInstanceOf[==>>[A, A]]

}

trait HasAvroPrimitiveConversions {

  implicit object toUtf8Conv extends (String ==>> Utf8) {
    def apply(a: String) = 
      if (a eq null) null
      else new Utf8(a)
  }

  implicit object fromUtf8Conv extends (Utf8 ==>> String) {
    def apply(a: Utf8) = 
      if (a eq null) null
      else a.toString
  }

  implicit object toByteBufferConv extends (Array[Byte] ==>> ByteBuffer) {
    def apply(a: Array[Byte]) = 
      if (a eq null) null
      else ByteBuffer.wrap(a)
  }

  implicit object fromByteBufferConv extends (ByteBuffer ==>> Array[Byte]) {
    def apply(a: ByteBuffer) = 
      if (a eq null) null
      else a.array
  }

  implicit object shortToIntConv extends (Short ==>> Int) {
    def apply(a: Short) = a.toInt
  }
  
  implicit object intToShortConv extends (Int ==>> Short) {
    def apply(a: Int) = a.toShort
  }

  implicit object byteToIntConv extends (Byte ==>> Int) {
    def apply(a: Byte) = a.toInt
  }
  
  implicit object intToByteConv extends (Int ==>> Byte) {
    def apply(a: Int) = a.toByte
  }

  implicit object charToIntConv extends (Char ==>> Int) {
    def apply(a: Char) = a.toInt
  }
  
  implicit object intToCharConv extends (Int ==>> Char) {
    def apply(a: Int) = a.toChar
  }

}

trait HasOptionConversions {

  implicit def toOption[FromElem, ToElem](implicit trfm: FromElem ==>> ToElem) = new (FromElem ==>> Option[ToElem]) {
    def apply(a: FromElem) = Option(a).map(trfm)
  }

  implicit def fromOption[FromElem, ToElem]
    (implicit trfm: FromElem ==>> ToElem) = new (Option[FromElem] ==>> ToElem) {
    def apply(a: Option[FromElem]) =
      if (a eq null) null.asInstanceOf[ToElem]
      else a.map(trfm).getOrElse(null).asInstanceOf[ToElem]
  }

}

trait HasTraversableConversions {

  implicit def toScalaTraversable[FromElem, ToColl, ToElem]
    (implicit ev: ToColl <:< Traversable[ToElem], trfm: FromElem ==>> ToElem, bf: BuilderFactory[ToElem, ToColl]) = new (GenericArray[FromElem] ==>> ToColl) {
    def apply(a: GenericArray[FromElem]) = {
      import scala.collection.JavaConversions._
      val res = a.iterator.map(trfm)
      val builder = bf.newBuilder
      builder ++= res
      builder.result
    }
  }

  implicit def fromScalaTraversable[FromColl, FromElem, ToElem]
    (implicit ev: FromColl <:< Traversable[FromElem], trfm: FromElem ==>> ToElem): FromColl ==>> GenericArray[ToElem] = new (FromColl ==>> GenericArray[ToElem]) {
    def apply(a: FromColl) = {
      if (a eq null) null
      else {
        val res = a.map(trfm)
        /* View Generic Array */
        new GenericArray[ToElem] {
          import scala.collection.JavaConversions._
          def add(elem: ToElem) = error("ARRAY IS IMMUTABLE")
          def clear = error("ARRAY IS IMMUTABLE")
          def peek = error("NOT SUPPORTED")
          def size = res.size
          def iterator = res.toIterator
          def getSchema = error("NOT SUPPORTED")
        }
      }
    }
  }

  implicit def toMap[M0, K0, V0, K1, V1]
    (implicit ev: M0 <:< Traversable[(K0, V0)], keyTrfm: K0 ==>> K1, valTrfm: V0 ==>> V1): M0 ==>> JMap[K1, V1] = new (M0 ==>> JMap[K1, V1]) {
    def apply(a: M0) = {
      if (a eq null) null
      else {
        /* TODO: don't use TreeMap, instead use an AbstractMap wrapping a */
        val rtn = new TreeMap[K1, V1]
        a.foreach(t => rtn.put(keyTrfm(t._1), valTrfm(t._2)))
        rtn
      }
    }
  }

  implicit def fromMap[K0, V0, M1, K1, V1]
    (implicit ev: M1 <:< Traversable[(K1, V1)], keyTrfm: K0 ==>> K1, valTrfm: V0 ==>> V1, bf: BuilderFactory[(K1, V1), M1]) = new (JMap[K0, V0] ==>> M1) {
    def apply(a: JMap[K0, V0]) = {
      import scala.collection.JavaConversions._
      val builder = bf.newBuilder
      builder ++= a.map(t => (keyTrfm(t._1), valTrfm(t._2)))
      builder.result
    }
  }

}
