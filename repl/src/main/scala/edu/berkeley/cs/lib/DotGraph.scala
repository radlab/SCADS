package edu.berkeley.cs.lib

import _root_.javax.servlet.http.Cookie
import _root_.net.liftweb.http._
import _root_.net.liftweb.util._
import _root_.net.liftweb.common._
import net.liftweb.http.provider.HTTPCookie


object DotGraph {
  def dispatchRules: LiftRules.DispatchPF = {
    case Req(List("dotgraph", graphId), "", GetRequest) =>
      () => null //TODO: Fix or delete this class
  }

  case class GraphPng(data: Array[Byte]) extends LiftResponse with HeaderDefaults {
    def toResponse = InMemoryResponse(data, ("Content-Length", data.length.toString) :: ("Content-Type", "image/png") :: headers, cookies, 200)
  }

  def getGraph(graphId: String): LiftResponse = {
    val proc = Runtime.getRuntime().exec("/usr/bin/dot -Tpng")
    val outstream = proc.getOutputStream()

    //FIX: or delete
    outstream.write(null)
    outstream.close()

    val instream = proc.getInputStream
    val graphStream = new java.io.ByteArrayOutputStream
    val buff = new Array[Byte](1024)
    var read = instream.read(buff)
    while(read > 0) {
      graphStream.write(buff, 0, read)
      read = instream.read(buff)
    }
    GraphPng(graphStream.toByteArray)
  }
}
