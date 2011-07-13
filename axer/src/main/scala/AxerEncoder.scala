package edu.berkeley.cs.scads.axer

import scala.collection.mutable.ArrayBuffer
import org.apache.avro.io.Encoder
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer;

class AxerEncoder(var buffer:ArrayBuffer[Byte]) extends Encoder {

  def length():Int = buffer.length

  def flush() {}

  /**
   * Reserves some space at the current point in the buffer for later
   * updates.  All this really does is add some zero bytes
   */
  def reserve(amount:Int) {
    for (_ <- (1 to amount))
      buffer += 0
  }

  /**
   * "Writes" a null value.  (Doesn't actually write anything, but
   * advances the state of the parser if this class is stateful.)
   * @throws AvroTypeException If this is a stateful writer and a
   *         null is not expected
   */
  def writeNull() {}
  
  /**
   * Write a boolean value.
   * @throws AvroTypeException If this is a stateful writer and a
   * boolean is not expected
   */
  def writeBoolean(b:Boolean) {
    if (b) 
      buffer += 1 
    else 
      buffer += 0
  }

  /**
   * Writes a 32-bit integer.
   * @throws AvroTypeException If this is a stateful writer and an
   * integer is not expected
   */
  def writeInt(value:Int) {
    buffer += ((value >>> 24).asInstanceOf[Byte],
               (value >>> 16).asInstanceOf[Byte],
               (value >>> 8).asInstanceOf[Byte],
               value.asInstanceOf[Byte])
  }

  // Writes an int into our buffer at a particular location
  def writeIntInto(value:Int, offset:Int) {
    buffer(offset)   = (value >>> 24).asInstanceOf[Byte]
    buffer(offset+1) = (value >>> 16).asInstanceOf[Byte]
    buffer(offset+2) = (value >>> 8).asInstanceOf[Byte]
    buffer(offset+3) = value.asInstanceOf[Byte]
  }

  /**
   * Write a 64-bit integer.
   * @throws AvroTypeException If this is a stateful writer and a
   * long is not expected
   */
  def writeLong(value:Long) {
    buffer += ((value >>> 56).asInstanceOf[Byte],
               (value >>> 48).asInstanceOf[Byte],
               (value >>> 40).asInstanceOf[Byte],
               (value >>> 32).asInstanceOf[Byte],
               (value >>> 24).asInstanceOf[Byte],
               (value >>> 16).asInstanceOf[Byte],
               (value >>> 8).asInstanceOf[Byte],
               value.asInstanceOf[Byte])
  }
  
  /** Write a float.
   * @throws IOException 
   * @throws AvroTypeException If this is a stateful writer and a
   * float is not expected
   */
  def writeFloat(value:Float) {
    writeInt(java.lang.Float.floatToRawIntBits(value))
  }

  /**
   * Write a double.
   * @throws AvroTypeException If this is a stateful writer and a
   * double is not expected
   */
  def writeDouble(value:Double) {
    writeLong(java.lang.Double.doubleToRawLongBits(value))
  }

  /**
   * Write a Unicode character string.
   * @throws AvroTypeException If this is a stateful writer and a
   * char-string is not expected
   */
  def writeString(value:Utf8) {
    writeBytes(value.getBytes)
  }
  
  /**
   * Write a byte string.
   * @throws AvroTypeException If this is a stateful writer and a
   *         byte-string is not expected
   */
  def writeBytes(bytes:ByteBuffer) {
    val pos = bytes.position();
    val start = bytes.arrayOffset() + pos;
    val len = bytes.limit() - pos;
    writeBytes(bytes.array(), start, len);
  }
  
  /**
   * Write a byte string.
   * @throws AvroTypeException If this is a stateful writer and a
   * byte-string is not expected
   */
  def writeBytes(bytes:Array[Byte], start:Int, len:Int) {
    buffer ++= bytes.view.slice(start,start+len)
  }

  /**
   * Writes a fixed size binary object.
   * @param bytes The contents to write
   * @param start The position within <tt>bytes</tt> where the contents
   * start.
   * @param len The number of bytes to write.
   * @throws AvroTypeException If this is a stateful writer and a
   * byte-string is not expected
   * @throws IOException
   */
  def writeFixed(bytes:Array[Byte], start:Int, len:Int) {
    writeBytes(bytes,start,len)
  }
  
  /**
   * Writes an enumeration.
   * @param e
   * @throws AvroTypeException If this is a stateful writer and an enumeration
   * is not expected or the <tt>e</tt> is out of range.
   * @throws IOException
   */
  def writeEnum(e:Int) {}

  /** Call this method to start writing an array.
   *
   *  When starting to serialize an array, call {@link
   *  #writeArrayStart}. Then, before writing any data for any item
   *  call {@link #setItemCount} followed by a sequence of
   *  {@link #startItem()} and the item itself. The number of
   *  {@link #startItem()} should match the number specified in
   *  {@link #setItemCount}.
   *  When actually writing the data of the item, you can call any {@link
   *  Encoder} method (e.g., {@link #writeLong}).  When all items
   *  of the array have been written, call {@link #writeArrayEnd}.
   *
   *  As an example, let's say you want to write an array of records,
   *  the record consisting of an Long field and a Boolean field.
   *  Your code would look something like this:
   *  <pre>
   *  out.writeArrayStart();
   *  out.setItemCount(list.size());
   *  for (Record r : list) {
   *    out.startItem();
   *    out.writeLong(r.longField);
   *    out.writeBoolean(r.boolField);
   *  }
   *  out.writeArrayEnd();
   *  </pre>
   *  @throws AvroTypeException If this is a stateful writer and an
   *          array is not expected
   */
  def writeArrayStart() {}

  /**
   * Call this method before writing a batch of items in an array or a map.
   * Then for each item, call {@link #startItem()} followed by any of the
   * other write methods of {@link Encoder}. The number of calls
   * to {@link #startItem()} must be equal to the count specified
   * in {@link #setItemCount}. Once a batch is completed you
   * can start another batch with {@link #setItemCount}.
   * 
   * @param itemCount The number of {@link #startItem()} calls to follow.
   * @throws IOException
   */
  def setItemCount(itemCount:Long) {
    if (itemCount > java.lang.Integer.MAX_VALUE)
      throw new Exception("Array too long")
  }
  
  /**
   * Start a new item of an array or map.
   * See {@link #writeArrayStart} for usage information.
   * @throws AvroTypeException If called outside of an array or map context
   */
  def startItem() {}

  /**
   * Call this method to finish writing an array.
   * See {@link #writeArrayStart} for usage information.
   *
   * @throws AvroTypeException If items written does not match count
   *          provided to {@link #writeArrayStart}
   * @throws AvroTypeException If not currently inside an array
   */
  def writeArrayEnd() {}

  /**
   * Call this to start a new map.  See
   * {@link #writeArrayStart} for details on usage.
   *
   * As an example of usage, let's say you want to write a map of
   * records, the record consisting of an Long field and a Boolean
   * field.  Your code would look something like this:
   * <pre>
   * out.writeMapStart();
   * out.setItemCount(list.size());
   * for (Map.Entry<String,Record> entry : map.entrySet()) {
   *   out.startItem();
   *   out.writeString(entry.getKey());
   *   out.writeLong(entry.getValue().longField);
   *   out.writeBoolean(entry.getValue().boolField);
   * }
   * out.writeMapEnd();
   * </pre>
   * @throws AvroTypeException If this is a stateful writer and a
   * map is not expected
   */
  def writeMapStart() {}

  /**
   * Call this method to terminate the inner-most, currently-opened
   * map.  See {@link #writeArrayStart} for more details.
   *
   * @throws AvroTypeException If items written does not match count
   *          provided to {@link #writeMapStart}
   * @throws AvroTypeException If not currently inside a map
   */
  def writeMapEnd() {}

  /** Call this method to write the tag of a union.
   *
   * As an example of usage, let's say you want to write a union,
   * whose second branch is a record consisting of an Long field and
   * a Boolean field.  Your code would look something like this:
   * <pre>
   * out.writeIndex(1);
   * out.writeLong(record.longField);
   * out.writeBoolean(record.boolField);
   * </pre>
   * @throws AvroTypeException If this is a stateful writer and a
   * map is not expected
   */
  def writeIndex(unionIndex:Int) {}

  /* Redirect output (and reset the parser state if we're checking).
   * Since we always write to our own buffer, this does nothing and
   * shouldn't be used */
  def init(out:java.io.OutputStream) {}

}
