package edu.berkeley.cs.snippet

import _root_.scala.xml._
import _root_.net.liftweb.http._
import _root_.net.liftweb.http.S._
import _root_.net.liftweb.http.SHtml._
import _root_.net.liftweb.mapper._
import _root_.net.liftweb.util.Helpers._
import _root_.net.liftweb.util._
import _root_.net.liftweb.common._

import net.liftweb.http.js._
import net.liftweb.http.js.JsCmds._
import net.liftweb.http.js.JE._

import edu.berkeley.cs.scads.piql.{Compiler, GraphViz}
import edu.berkeley.cs.scads.piql.parser.{BoundSpec, BoundQuery}

object PiqlSpec extends SessionVar[Box[BoundSpec]](Empty)

object PiqlGraphs {
  private val curId = new java.util.concurrent.atomic.AtomicInteger
  private val graphs = new scala.collection.mutable.HashMap[Int, String]

  def addGraph(dotCode: String): Int = {
    val id = curId.getAndIncrement()
    graphs.put(id, dotCode)
    return id
  }

  def getGraph(id: Int): String = graphs(id)
}

class PiqlEditor {
  val defaultPiql = Compiler.readFile(new java.io.File("../piql/src/test/resources/scadr.scads"))

  protected def validatePiql(code: String): JsCmd = {
    val spec = Compiler.getOptimizedSpec(code)
    val grapher = new GraphViz(spec.entities.values.toList)
    PiqlSpec.set(Full(spec))

    def mkQueryLink(query: BoundQuery) = {
      val aPlan = grapher.getAnnotatedPlan(query.plan)
      val recCount = aPlan.recCount match {
        case Some(c) => <span>{c + "Records Max"}</span>
        case None => <font color="red">UNBOUNDED</font>
      }
      val graphId = PiqlGraphs.addGraph(aPlan.dotCode)
      <li><a href={"/dotgraph/" + graphId} class="highslide" onclick="return hs.expand(this)">{query.name}</a> - {recCount}</li>
    }

    val entityQueries = spec.entities.values.map(e => <li>{e.name}</li><ul>{e.queries.values.map(mkQueryLink)}</ul>)
    val otherQueries = <li>Static Queries</li><ul>{spec.orphanQueries.values.map(mkQueryLink)}</ul>;
    SetHtml("status", <ul>{entityQueries}{otherQueries}</ul>)
  }

  def editor(xhtml: NodeSeq): NodeSeq = {
    bind("edit", xhtml,
      "editor" -> SHtml.textarea(defaultPiql, (t: String) => (), "id" -> "piqlcode"),
      "validate" -> <button onclick={SHtml.ajaxCall(ValById("piqlcode"), validatePiql _)._2}>Validate PIQL</button>)
  }
}
