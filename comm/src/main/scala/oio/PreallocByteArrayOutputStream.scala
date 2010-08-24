package edu.berkeley.cs.scads.comm
package oio

import java.io._

/**
 * Requires preAlloc be less than initSize
 */
class PreallocByteArrayOutputStream(preAlloc: Int, initSize: Int)
  extends ByteArrayOutputStream(initSize) {
  require(preAlloc >= 0 && preAlloc < initSize)
  count = preAlloc /* Do the pre-allocation */

  def this(preAlloc: Int) = this(preAlloc, preAlloc + 32)

  def numPreAllocBytes = preAlloc

  def effectiveSize = size - preAlloc

  def underlyingBuffer = buf
}
