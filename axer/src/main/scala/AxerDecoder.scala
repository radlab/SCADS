package edu.berkeley.cs.scads.axer

import org.apache.avro.io.Decoder
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer;

case class AxerDecoder(var bytes:Array[Byte]) extends Decoder {

  var recBase:Int = 0
  def setRecBase(b:Int) {
    recBase = b
  }

  var offset:Int = 0
  def setOffset(o:Int) {
    offset = o + recBase
  }

  /**
   * "Reads" a null value.  (Doesn't actually read anything, but
   * advances the state of the parser if the implementation is
   * stateful.)
   *  @throws AvroTypeException If this is a stateful reader and
   *          null is not the type of the next value to be read
   */
  def readNull() {}

  /**
   * Reads a boolean value written by {@link Encoder#writeBoolean}.
   * @throws AvroTypeException If this is a stateful reader and
   * boolean is not the type of the next value to be read
   */

  def readBoolean():Boolean = {
    bytes(offset) == 1
  }

  /**
   * Reads an integer written by {@link Encoder#writeInt}.
   * @throws AvroTypeException If encoded value is larger than
   *          32-bits
   * @throws AvroTypeException If this is a stateful reader and
   *          int is not the type of the next value to be read
   */
  def readInt():Int = {
    (
      (0xff & bytes(offset)) << 24 |
      (0xff & bytes(offset+1)) << 16 |
      (0xff & bytes(offset+2)) << 8 |
      (0xff & bytes(offset+3)) << 0
    ).asInstanceOf[Int]     
  }

  /**
   * Reads a long written by {@link Encoder#writeLong}.
   * @throws AvroTypeException If this is a stateful reader and
   *          long is not the type of the next value to be read
   */
  def readLong():Long = {
    (
      (0xff & bytes(offset)).asInstanceOf[Long] << 56 |
      (0xff & bytes(offset+1)).asInstanceOf[Long] << 48 |
      (0xff & bytes(offset+2)).asInstanceOf[Long] << 40 |
      (0xff & bytes(offset+3)).asInstanceOf[Long] << 32 |
      (0xff & bytes(offset+4)).asInstanceOf[Long] << 24 |
      (0xff & bytes(offset+5)).asInstanceOf[Long] << 16 |
      (0xff & bytes(offset+6)).asInstanceOf[Long] << 8 |
      (0xff & bytes(offset+7)).asInstanceOf[Long] << 0
    ).asInstanceOf[Long]
  }

  /**
   * Reads a float written by {@link Encoder#writeFloat}.
   * @throws AvroTypeException If this is a stateful reader and
   * is not the type of the next value to be read
   */
  def readFloat():Float = {
    val i = readInt
    java.lang.Float.intBitsToFloat(i).asInstanceOf[Float]
  }

  /**
   * Reads a double written by {@link Encoder#writeDouble}.
   * @throws AvroTypeException If this is a stateful reader and
   *           is not the type of the next value to be read
   */
  def readDouble():Double = {
    val l = readLong
    java.lang.Double.longBitsToDouble(l).asInstanceOf[Double]
  }
    
  /**
   * Reads a char-string written by {@link Encoder#writeString}.
   * @throws AvroTypeException If this is a stateful reader and
   * char-string is not the type of the next value to be read
   */
  def readString(old:Utf8):Utf8 = {
    val ret = if (old == null) new Utf8 else old
    ret.setLength(varLen)
    readFixed(ret.getBytes,0,varLen)
    ret
  }

  /**
   * Reads a byte-string written by {@link Encoder#writeBytes}.
   * if <tt>old</tt> is not null and has sufficient capacity to take in
   * the bytes being read, the bytes are returned in <tt>old</tt>.
   * @throws AvroTypeException If this is a stateful reader and
   *          byte-string is not the type of the next value to be read
   */
  def readBytes(old:ByteBuffer):ByteBuffer = {
    val result =
      if (old != null && varLen <= old.capacity()) {
        old.clear
        old
      }
      else
        ByteBuffer.allocate(varLen);
    readFixed(result.array,result.position,varLen)
    result
  }

  
  /**
   * Reads fixed sized binary object.
   * @param bytes The buffer to store the contents being read.
   * @param start The position where the data needs to be written.
   * @param length  The size of the binary object.
   * @throws AvroTypeException If this is a stateful reader and
   *          fixed sized binary object is not the type of the next
   *          value to be read or the length is incorrect.
   * @throws IOException
   */
  def readFixed(dest:Array[Byte], start:Int, length:Int) {
    Array.copy(bytes,offset,dest,start,length)
  }

  /**
   * Reads an enumeration.
   * @return The enumeration's value.
   * @throws AvroTypeException If this is a stateful reader and
   *          enumeration is not the type of the next value to be read.
   * @throws IOException
   */
  def readEnum():Int = {
    throw new Exception("No enum yet")
    0
  }

  // Call to setVarOffset will set this so that reads can do the right thing
  var varLen:Int = 0

  /*
   * Utitlity method to set the offset to a particular
   * named field
   */
  def setOffsetVar(name:String, schema:AxerSchema) {
    val vpos = schema.getVarPosition(name)
    if (vpos < 0) 
      throw new Exception("Field name is not a variable length field in this schema: "+name)
    offset = schema.constOffset+(vpos*4)
    val sp = readInt
    offset =
      if (vpos == (schema.varFields.length - 1))
        0
      else
        offset+4
    varLen = readInt - sp
    offset = sp
  }
  
  /**
   * Reads and returns the size of the first block of an array.  If
   * this method returns non-zero, then the caller should read the
   * indicated number of items, and then call {@link
   * #arrayNext} to find out the number of items in the next
   * block.  The typical pattern for consuming an array looks like:
   * <pre>
   *   for(long i = in.readArrayStart(); i != 0; i = in.arrayNext()) {
   *     for (long j = 0; j < i; j++) {
   *       read next element of the array;
   *     }
   *   }
   * </pre>
   *  @throws AvroTypeException If this is a stateful reader and
   *          array is not the type of the next value to be read */
  def readArrayStart():Long = {
    varLen
  }

  /**
   * Processes the next block of an array and returns the number of items in
   * the block and let's the caller
   * read those items.
   * @throws AvroTypeException When called outside of an
   *         array context
   */
  def arrayNext():Long = 0


  /**
   * Reads and returns the size of the next block of map-entries.
   * Similar to {@link #readArrayStart}.
   *
   *  As an example, let's say you want to read a map of records,
   *  the record consisting of an Long field and a Boolean field.
   *  Your code would look something like this:
   * <pre>
   *   Map<String,Record> m = new HashMap<String,Record>();
   *   Record reuse = new Record();
   *   for(long i = in.readMapStart(); i != 0; i = in.readMapNext()) {
   *     for (long j = 0; j < i; j++) {
   *       String key = in.readString();
   *       reuse.intField = in.readInt();
   *       reuse.boolField = in.readBoolean();
   *       m.put(key, reuse);
   *     }
   *   }
   * </pre>
   * @throws AvroTypeException If this is a stateful reader and
   *         map is not the type of the next value to be read
   */
  def readMapStart():Long = 0

  /**
   * Processes the next block of map entries and returns the count of them.
   * Similar to {@link #arrayNext}.  See {@link #readMapStart} for details.
   * @throws AvroTypeException When called outside of a
   *         map context
   */
  def mapNext():Long = 0


  /**
   * Reads the tag of a union written by {@link Encoder#writeIndex}.
   * @throws AvroTypeException If this is a stateful reader and
   *         union is not the type of the next value to be read
   */
  def readIndex():Int = {
    throw new Exception("No index yet")
  }

  /*
   * Skip methods all do nothing in Axer as you can't skip things
   */
  def skipString() {}
  def skipBytes() {}
  def skipFixed(len:Int) {}
  def skipMap():Long = {0}
  def skipArray():Long = {0}

  // this is slow and shouldn't be used, but is here for api compatibility
  def init(in:java.io.InputStream) {
    var builder = new scala.collection.mutable.ArrayBuilder.ofByte
    var b:Byte = in.read.asInstanceOf[Byte]
    while(b != -1) {
      builder += b
      b = in.read.asInstanceOf[Byte]
    }
    bytes = builder.result
  }

}
