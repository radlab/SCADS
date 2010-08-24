package edu.berkeley.cs.scads.comm
package netty

import java.io._

/**
 * Lets us get access to the underlying buffer without a copy
 */
class ExposingByteArrayOutputStream(size: Int) 
  extends ByteArrayOutputStream(size) {
  def this() = this(32)
  def getUnderlying = buf
}
