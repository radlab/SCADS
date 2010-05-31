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

import edu.berkeley.cs.scads.piql.Compiler
import edu.berkeley.cs.scads.piql.parser.BoundSpec

object PiqlSpec extends SessionVar[Box[BoundSpec]](Empty)

class PiqlEditor {
  val defaultPiql = Compiler.readFile(new java.io.File("../piql/src/test/resources/scadr.scads"))

  protected def validatePiql(code: String): JsCmd = {
    val spec = Compiler.getOptimizedSpec(code)
    PiqlSpec.set(Full(spec))

    def mkQueryLink(queryName: String) = <a href={"/dotgraph/" + queryName}>{queryName}</a>
    val result =
      <ul>
        <span>{
          spec.entities.values.map(e => {
              <li>{e.name}</li>
              <ul>{e.queries.keys.map(q => <li>{mkQueryLink(q)}</li>)}</ul>
          })}
        </span>
          <li>Other Queries</li>
          <ul>{spec.orphanQueries.keys.map(q => <li>{mkQueryLink(q)}</li>)}</ul>
      </ul>
    SetHtml("status", result)
  }

  def editor(xhtml: NodeSeq): NodeSeq = {
    bind("edit", xhtml,
      "editor" -> SHtml.textarea(defaultPiql, (t: String) => (), "id" -> "piqlcode"),
      "validate" -> <button onclick={SHtml.ajaxCall(ValById("piqlcode"), validatePiql _)._2}>Validate PIQL</button>)
  }
}
