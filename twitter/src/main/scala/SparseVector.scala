import scala.collection.JavaConversions._
import java.io.{Externalizable, ObjectInput, ObjectOutput, ObjectInputStream, ObjectOutputStream, IOException}
import cern.colt.map.OpenIntDoubleHashMap
import java.security.MessageDigest


//class SparseVector[Key](@transient var elements: java.util.HashMap[Key, Double]) extends Externalizable with Logging {
@serializable class SparseVector(var elements: OpenIntDoubleHashMap) {


  def this() {
   this(new OpenIntDoubleHashMap())
  }

  val hashLength = 4

  def doHash(s: String): Int = {
    val dg = SparseVector.md.digest(s.toArray.map(_.toByte))
    var res = 0
    for (j <- hashLength to 1 by -1) {
      res += (res << 8) + dg(dg.size - j)
    }

    if (s.matches("^(real_).*")) {
      res | 0x80000000
    }
    else {
      res & 0x7fffffff
    }
  }

  def apply(feature: Int): Double = elements.get(feature)

  def apply(feature: String): Double = elements.get(doHash(feature))

  def update(feature: Int, value: Double): Unit = { elements.put(feature, value) }

  def update(feature: String, value: Double): Unit = { elements.put(doHash(feature), value) }

  def size = elements.size

  def += (other: SparseVector): SparseVector = {
    for (k <- other.keys) {
      this(k) = this(k) + other(k)
    }

    return this
  }

  def -= (other: SparseVector): SparseVector = {
    for (k <- other.keys) {
      this(k) = this(k) - other(k)
    }
    return this
  }

  def dot(other: SparseVector): Double = {
    var ans = 0.0

    if (this.size > other.size) {
      for (k <- other.keys) {
        ans += this(k) * other(k)
      }
    }
    else {
      for (k <- this.keys) {
        ans += this(k) * other(k)
      }
    }
    return ans
  }

  def * ( scale: Double): SparseVector = {
    //val newElements = new java.util.HashMap[Key, Double]()
    val newElements = SparseVector.zeros
    for (k <- keys) {
      val v = this(k)
      val tempVal = v * scale
      if (tempVal != 0.0) {
        newElements(k) = v * scale
      }
    }
    newElements
  }

  def unary_- = this * -1

  def keys = {
    val l: cern.colt.list.IntArrayList = elements.keys
    l.elements
  }

  //override def toString = elements.mkString("(", ", ", ")")

/*
  @throws(classOf[IOException])
  def writeExternal(o: ObjectOutput) {
    //o.defaultWriteObject
    o.writeInt(elements.size)
    for ((k, v) <- elements) {
      o.writeUTF(k)
      o.writeDouble(v)
    }
  }

  @throws(classOf[IOException])
  def readExternal(i: ObjectInput) {
    //i.defaultReadObject
    val n = i.readInt
    for (j <- 1 to n) {
      val k = i.readUTF
      val v = i.readDouble
      elements(k) = v
    }
  }
  */

}

object SparseVector {
  def zeros = new SparseVector(new OpenIntDoubleHashMap())

  def zero = zeros

  class Multiplier(num: Double) {
    def * (vec: SparseVector) = vec * num
  }

  val md = MessageDigest.getInstance("MD5")

  implicit def doubleToMultiplier(num: Double) = new Multiplier(num)
}
