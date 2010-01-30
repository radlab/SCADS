package edu.berkeley.cs.scads.comm

/**
 * Not thread safe
 */
class CircularByteBuffer(private var initialSize: Int) {
    private var bytes = new Array[Byte](initialSize)
    private var head = 0
    private var _size = 0

    def this() = this(64*1024)

    def append(toAppend: Array[Byte]) = {
        if (bytesRemaining < toAppend.length) {
            // expand
            val newSize = Math.max(bytes.length*2, (_size+toAppend.length)*2)
            val newBytes = new Array[Byte](newSize) 
            if (head+size < bytes.length+1)
                System.arraycopy(bytes, head, newBytes, 0, _size) 
            else {
                System.arraycopy(bytes, head, newBytes, 0, bytes.length-head)
                System.arraycopy(bytes, 0, newBytes, bytes.length-head, _size-(bytes.length-head))
            }
            head = 0
            bytes = newBytes
        }
        val tailStart = (head + _size) % bytes.length
        // copy until end
        val toCopy = Math.min(toAppend.length, bytes.length-tailStart)
        System.arraycopy(toAppend, 0, bytes, tailStart, toCopy)
        
        // now copy into beginning
        if (toCopy < toAppend.length)
            System.arraycopy(toAppend, toCopy, bytes, 0, toAppend.length-toCopy)
        _size += toAppend.length
    }

    def consumeBytes(length: Int):Array[Byte] = {
        if (length > _size) 
            throw new IllegalArgumentException("Cannot consume more bytes than there are")
        if (length < 0)
            throw new IllegalArgumentException("Cannot consume negative bytes")
        val rtn = new Array[Byte](length)
        if (length == 0)
            return rtn
        if (head + length < bytes.length + 1)
            System.arraycopy(bytes, head, rtn, 0, length)
        else {
            System.arraycopy(bytes, head, rtn, 0, bytes.length-head)
            System.arraycopy(bytes, 0, rtn, bytes.length-head, length-(bytes.length-head))
        }
        head = (head + length) % bytes.length
        _size -= length
        rtn
    }

    def consumeInt:Int = {
        val bytea = consumeBytes(4)
        // little endian
        // (bytea(3).toInt & 0xFF) << 24 | (bytea(2).toInt & 0xFF) << 16 | (bytea(1).toInt & 0xFF) << 8 | (bytea(0).toInt & 0xFF)
        // big endian
        (bytea(0).toInt & 0xFF) << 24 | (bytea(1).toInt & 0xFF) << 16 | (bytea(2).toInt & 0xFF) << 8 | (bytea(3).toInt & 0xFF)
    }

    def bytesRemaining:Int = bytes.length - _size

    def size:Int = _size
    
    def isEmpty:Boolean = _size == 0
}
