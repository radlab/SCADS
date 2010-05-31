package edu.berkeley.cs.lib

import _root_.javax.servlet.http.Cookie
import _root_.net.liftweb.http._
import _root_.net.liftweb.util._
import _root_.net.liftweb.common._
import net.liftweb.http.provider.HTTPCookie


object DotGraph {
  def dispatchRules: LiftRules.DispatchPF = {
    case Req(List("dotgraph", graphId), "", GetRequest) =>
      () => Full(getGraph(graphId))
  }

  case class GraphPng(data: Array[Byte]) extends LiftResponse with HeaderDefaults {
    def toResponse = InMemoryResponse(data, ("Content-Length", data.length.toString) :: ("Content-Type", "image/png") :: headers, cookies, 200) 
  }
  
  def getGraph(graphId: String): LiftResponse = {
    val proc = Runtime.getRuntime().exec("/Applications/Graphviz.app/Contents/MacOS/dot -Tpng /Users/marmbrus/Workspace/scads/piql/thoughtstream.dot")
    val instream = proc.getInputStream
    val outstream = new java.io.ByteArrayOutputStream

    val buff = new Array[Byte](1024)
    var read = instream.read(buff)
    while(read > 0) {
      outstream.write(buff, 0, read)
      read = instream.read(buff)
    }
    GraphPng(outstream.toByteArray)
  }
}
