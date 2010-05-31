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

class PiqlEditor {
  val defaultPiql = Compiler.readFile(new java.io.File("../piql/src/test/resources/scadr.scads"))

  protected def validatePiql(code: String): JsCmd = {
    val spec = Compiler.getOptimizedSpec(code)
    SetHtml("status", spec.entities.map(_._1).mkString)
  }

  def editor(xhtml: NodeSeq): NodeSeq = {
    bind("edit", xhtml,
      "editor" -> SHtml.textarea(defaultPiql, (t: String) => (), "id" -> "piqlcode"),
      "validate" -> <button onclick={SHtml.ajaxCall(ValById("piqlcode"), validatePiql _)._2}>Validate PIQL</button>)
  }
}
