package edu.berkeley.cs.scads.comm

import java.nio.channels.SocketChannel

trait WriteCallback {
    def writeFinished
}

trait ChannelHandler {
    def processData(socket: SocketChannel, data: Array[Byte], count: Int)

    def handleException(exception: Exception)
}
