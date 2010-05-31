package edu.berkeley.cs.lib

import _root_.javax.servlet.http.Cookie
import _root_.net.liftweb.http._
import _root_.net.liftweb.util._
import _root_.net.liftweb.common._
import net.liftweb.http.provider.HTTPCookie

import edu.berkeley.cs.snippet.PiqlSpec
import edu.berkeley.cs.scads.piql.GraphViz
import edu.berkeley.cs.scads.piql.QueryPlan

object DotGraph {
  def dispatchRules: LiftRules.DispatchPF = {
    case Req(List("dotgraph", graphId), "", GetRequest) =>
      () => Full(getGraph(graphId))
  }

  case class GraphPng(data: Array[Byte]) extends LiftResponse with HeaderDefaults {
    def toResponse = InMemoryResponse(data, ("Content-Length", data.length.toString) :: ("Content-Type", "image/png") :: headers, cookies, 200) 
  }
  
  def getGraph(graphId: String): LiftResponse = {
    val proc = Runtime.getRuntime().exec("/Applications/Graphviz.app/Contents/MacOS/dot -Tpng")
    val outstream = proc.getOutputStream()

    val spec = PiqlSpec.open_!
    val grapher = new GraphViz(spec.entities.values.toList)
    outstream.write(grapher(getPlan(graphId)).getBytes)
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

  //TODO: Cleanup / handle dup query names
  private def getPlan(name: String): QueryPlan = {
    val spec = PiqlSpec.open_!
    spec.entities.values.foreach(e => {
      e.queries.get(name) match {
      case Some(q) => return q.plan
      case None =>
    }})

    spec.orphanQueries(name).plan
  }
}
