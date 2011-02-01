import scala.collection.JavaConversions._
import java.io.{Externalizable, ObjectInput, ObjectOutput, ObjectInputStream, ObjectOutputStream, IOException}


class SparseVectorOriginal(@transient var elements: java.util.HashMap[String, Double]) extends Externalizable {

  def this() {
   this(new java.util.HashMap[String, Double]())
  }

  override def clone = new SparseVectorOriginal(elements.clone.asInstanceOf[java.util.HashMap[String, Double]])

  def apply(feature: String): Double = elements.getOrElse(feature, 0)

  def update(feature: String, value: Double): Unit = {
    if (value != 0.0) {
      elements(feature) = value
    }
    else {
      elements -= feature
    }
  }

  def size = elements.size

  def head = elements.head

  def + (other: SparseVectorOriginal): SparseVectorOriginal = {
    val result = SparseVectorOriginal.zeros
    result += this
    result += other
    return result
  }

  def += (other: SparseVectorOriginal): SparseVectorOriginal = {
    for (k <- other.elements.keySet) {
      this(k) = this(k) + other(k)
    }

    return this
  }

  def -= (other: SparseVectorOriginal): SparseVectorOriginal = {
    for (k <- other.elements.keySet) {
      this(k) = this(k) - other(k)
    }
    return this
  }

  def dot(other: SparseVectorOriginal): Double = {
    var ans = 0.0

    if (this.size > other.size) {
      for (k <- other.elements.keySet) {
        ans += this(k) * other(k)
      }
    }
    else {
      for (k <- this.elements.keySet) {
        ans += this(k) * other(k)
      }
    }
    return ans
  }

  def * ( scale: Double): SparseVectorOriginal = {
    val newElements = SparseVectorOriginal.zeros
    if (scale != 0.0) {
      for ((k, v) <- elements) {
        val res = v * scale
        if (res != 0.0) {
          newElements(k) = res
        }
      }
    }
    newElements
  }

  def keys = elements.keys

  def filter(p: ((String, Double)) => Boolean): SparseVectorOriginal = {
    val newElements = SparseVectorOriginal.zeros
    for ((k, v) <- elements) {
      if (p((k, v))) {
        newElements(k) = v
      }
    }
    newElements
  }

  def groupBy[K](p: ((String, Double)) => K): scala.collection.mutable.HashMap[K, SparseVectorOriginal] = {
    val groups = scala.collection.mutable.HashMap[K, SparseVectorOriginal]()
    for ((k, v) <- elements) {
      val idx = p((k, v))
      if (!groups.contains(idx)) {
        groups(idx) = SparseVectorOriginal.zeros
      }
      groups(idx)(k) = v
    }
    groups
  }

  def unary_- = this * -1
  
  override def toString = elements.mkString("(", ", ", ")")

  @throws(classOf[IOException])
  def writeExternal(o: ObjectOutput) {
    //o.defaultWriteObject
    o.writeInt(elements.size)
    for ((k, v) <- elements) {
      o.writeInt(k.size)
      for (c <- k) { o.writeChar(c) }
      //o.writeUTF(k)
      o.writeDouble(v)
    }
  }

  @throws(classOf[IOException])
  def readExternal(i: ObjectInput) {
    //i.defaultReadObject
    val n = i.readInt
    for (j <- 1 to n) {
      val len = i.readInt
      var k = new Array[Char](len)
      for (z <- 0 until len) {
        k(z) = i.readChar
      }
      //val k = i.readUTF
      val v = i.readDouble
      elements(new String(k)) = v
    }
  }

}

object SparseVectorOriginal {
  def zeros = new SparseVectorOriginal(new java.util.HashMap[String, Double]())

  def zero = zeros

  class Multiplier(num: Double) {
    def * (vec: SparseVectorOriginal) = vec * num
  }

  implicit def doubleToMultiplier(num: Double) = new Multiplier(num)
}
