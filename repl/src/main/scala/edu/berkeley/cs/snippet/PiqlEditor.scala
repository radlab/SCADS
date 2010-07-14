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
  val defaultPiql = Compiler.readInputStream(Thread.currentThread().getContextClassLoader().getResourceAsStream("scadr.piql"))

  protected def validatePiql(code: String): JsCmd = {
    val spec =
      try {
        Compiler.getOptimizedSpec(code)
      } catch {
        case _ => return Alert("Invalid PIQL spec")
      }

    val grapher = new GraphViz(spec.entities.values.toList)
    PiqlSpec.set(Full(spec))

    def mkQueryLink(location: String, query: BoundQuery): NodeSeq = {
      val aPlan = grapher.getAnnotatedPlan(query.plan)
      val recCount = aPlan.recCount match {
        case Some(c) => <span>{c}</span>
        case None => <font color="red">UNBOUNDED</font>
      }
      val opCount = aPlan.ops match {
        case Some(o) => <span>{o}</span>
        case None => <font color="red">UNBOUNDED</font>
      }
      val graphId = PiqlGraphs.addGraph(aPlan.dotCode)
      <tr><td><a href={"/dotgraph/" + graphId} class="highslide" onclick="return hs.expand(this)">{query.name}</a></td><td>{location}</td><td>{recCount}</td><td>{opCount}</td></tr>
    }

    val entityQ = spec.entities.values.flatMap(e => e.queries.values.map(mkQueryLink(e.name, _))).toList
    val entityQueries: NodeSeq =
      if (entityQ.isEmpty)
        <span></span>
      else
        entityQ.reduceLeft(_++_)
    val otherQueries: NodeSeq =
      if (spec.orphanQueries.isEmpty)
        <span></span>
      else
        spec.orphanQueries.values.map(mkQueryLink("static", _)).reduceLeft(_++_)
    SetHtml("status", <tr><td>Plan</td><td>Location</td><td>Recs</td><td>Ops</td></tr> ++ entityQueries ++ otherQueries)
  }

  def editor(xhtml: NodeSeq): NodeSeq = {
    bind("edit", xhtml,
      "editor" -> SHtml.textarea(defaultPiql, (t: String) => (), "id" -> "piqlcode"),
      "validate" -> <button onclick={SHtml.ajaxCall(ValById("piqlcode"), validatePiql _)._2}>Validate PIQL</button>)
  }
}
