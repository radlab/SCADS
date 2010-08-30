package com.googlecode.avro
package runtime

import org.apache.avro.Schema
import Schema.Type
import org.apache.avro.generic.{ GenericArray, GenericData }
import org.apache.avro.util.Utf8

import java.nio.ByteBuffer

import scala.collection.Traversable
import scala.collection.mutable.ArrayBuffer

import java.util.{ Map => JMap, Iterator => JIterator, AbstractMap, AbstractSet }

/**
 * http://stackoverflow.com/questions/2257341/java-scala-deep-collections-interoperability
 */
trait ==>>[A, B] extends (A => B) {
  def apply(a: A): B
}

object BasicTransforms {

  object IdentityTransform extends (Any ==>> Any) {
    def apply(a: Any) = a
  }

  object toUtf8Conv extends (String ==>> Utf8) {
    def apply(a: String) = 
      if (a eq null) null
      else new Utf8(a)
  }

  object fromUtf8Conv extends (Utf8 ==>> String) {
    def apply(a: Utf8) = 
      if (a eq null) null
      else a.toString
  }

  object toByteBufferConv extends (Array[Byte] ==>> ByteBuffer) {
    def apply(a: Array[Byte]) = 
      if (a eq null) null
      else ByteBuffer.wrap(a)
  }

  object fromByteBufferConv extends (ByteBuffer ==>> Array[Byte]) {
    def apply(a: ByteBuffer) = 
      if (a eq null) null
      else a.array
  }

  import java.lang.{ Boolean => JBoolean, Short => JShort, Byte => JByte, Character => JCharacter, 
                     Integer => JInteger, Long => JLong, Float => JFloat, Double => JDouble }

  object shortToJIntConv extends (Short ==>> JInteger) {
    def apply(a: Short) = JInteger.valueOf(a.toInt)
  }

  object boxedShortToJIntConv extends (JShort ==>> JInteger) {
    def apply(a: JShort) = 
      if (a eq null) null
      else JInteger.valueOf(a.intValue)
  }

  object jintToShortConv extends (JInteger ==>> Short) {
    def apply(a: JInteger) = a.shortValue
  }

  object jintToBoxedShortConv extends (JInteger ==>> JShort) {
    def apply(a: JInteger) = 
      if (a eq null) null
      else JShort.valueOf(a.shortValue)
  }

  object byteToJIntConv extends (Byte ==>> JInteger) {
    def apply(a: Byte) = JInteger.valueOf(a.toInt)
  }

  object boxedByteToJIntConv extends (JByte ==>> JInteger) {
    def apply(a: JByte) = 
      if (a eq null) null
      else JInteger.valueOf(a.intValue)
  }

  object jintToByteConv extends (JInteger ==>> Byte) {
    def apply(a: JInteger) = a.byteValue
  }

  object jintToBoxedByteConv extends (JInteger ==>> JByte) {
    def apply(a: JInteger) = 
      if (a eq null) null
      else JByte.valueOf(a.byteValue)
  }

  object charToJIntConv extends (Char ==>> JInteger) {
    def apply(a: Char) = JInteger.valueOf(a.toInt)
  }

  object boxedCharToJIntConv extends (JCharacter ==>> JInteger) {
    // TODO: is there a better way to do this one?
    def apply(a: JCharacter) = 
      if (a eq null) null
      else JInteger.valueOf(a.charValue.toInt)
  }

  object jintToCharConv extends (JInteger ==>> Char) {
    // TODO: is there a better way to do this one?
    def apply(a: JInteger) = a.intValue.toChar
  }

  object jintToBoxedCharConv extends (JInteger ==>> JCharacter) {
    // TODO: is there a better way to do this one?
    def apply(a: JInteger) = 
      if (a eq null) null
      else JCharacter.valueOf(a.intValue.toChar)
  }

  object intToJIntConv extends (Int ==>> JInteger) {
    def apply(a: Int) = JInteger.valueOf(a)
  }

  object jintToIntConv extends (JInteger ==>> Int) {
    def apply(a: JInteger) = a.intValue
  }

  object longToJLongConv extends (Long ==>> JLong) {
    def apply(a: Long) = JLong.valueOf(a)
  }

  object jlongToLongConv extends (JLong ==>> Long) {
    def apply(a: JLong) = a.longValue
  }

  object floatToJFloatConv extends (Float ==>> JFloat) {
    def apply(a: Float) = JFloat.valueOf(a)
  }

  object jfloatToFloatConv extends (JFloat ==>> Float) {
    def apply(a: JFloat) = a.floatValue
  }

  object doubleToJDoubleConv extends (Double ==>> JDouble) {
    def apply(a: Double) = JDouble.valueOf(a)
  }

  object jdoubleToDoubleConv extends (JDouble ==>> Double) {
    def apply(a: JDouble) = a.doubleValue
  }

  object booleanToJBooleanConv extends (Boolean ==>> JBoolean) {
    def apply(a: Boolean) = JBoolean.valueOf(a)
  }

  object jbooleanToBooleanConv extends (JBoolean ==>> Boolean) {
    def apply(a: JBoolean) = a.booleanValue
  }

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

  import BasicTransforms._

  def convert[A, B](a: A)(implicit trfm: A ==>> B): B = a

  implicit def identity[A] =
    IdentityTransform.asInstanceOf[==>>[A, A]]

}

trait HasAvroPrimitiveConversions {

  import BasicTransforms._

  implicit def stringToUtf8 = toUtf8Conv 
  implicit def utf8ToString = fromUtf8Conv

  implicit def byteArrayToByteBuffer = toByteBufferConv
  implicit def byteBufferToByteArray = fromByteBufferConv

  implicit def shortToJInt  = shortToJIntConv
  implicit def jshortToJInt = boxedShortToJIntConv
  implicit def jintToShort  = jintToShortConv
  implicit def jintToJShort = jintToBoxedShortConv

  implicit def byteToJInt   = byteToJIntConv
  implicit def jbyteToJInt  = boxedByteToJIntConv
  implicit def jintToByte   = jintToByteConv
  implicit def jintToJByte  = jintToBoxedByteConv

  implicit def charToJInt   = charToJIntConv
  implicit def jcharToJInt  = boxedCharToJIntConv
  implicit def jintToChar   = jintToCharConv
  implicit def jintToJChar  = jintToBoxedCharConv

  implicit def intToJInt = intToJIntConv
  implicit def jintToInt = jintToIntConv

  implicit def longToJLong = longToJLongConv
  implicit def jlongToLong = jlongToLongConv

  implicit def floatToJFloat = floatToJFloatConv
  implicit def jfloatToFloat = jfloatToFloatConv

  implicit def doubleToJDouble = doubleToJDoubleConv
  implicit def jdoubleToDouble = jdoubleToDoubleConv

  implicit def booleanToJBoolean = booleanToJBooleanConv
  implicit def jbooleanToBoolean = jbooleanToBooleanConv

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
        import scala.collection.JavaConversions._
        new AbstractMap[K1, V1] {
          def entrySet = new AbstractSet[JMap.Entry[K1,V1]] {
            def iterator =
              a.map(t => new AbstractMap.SimpleImmutableEntry[K1, V1](keyTrfm(t._1), valTrfm(t._2))).toIterator
            def size = a.size
          }
        }
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
