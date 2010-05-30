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


class PiqlEditor {
  protected def validatePiql(code: String): JsCmd = {
    println("running validator")
    SetHtml("status", "Valid PIQL!")
  }

  def editor(xhtml: NodeSeq): NodeSeq = {
    bind("edit", xhtml,
      "editor" -> SHtml.textarea("", (t: String) => (), "id" -> "piqlcode"),
      "validate" -> <button onclick={SHtml.ajaxCall(ValById("piqlcode"), validatePiql _)._2}>Validate PIQL</button>)
  }
}
