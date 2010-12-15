package edu.berkeley.cs.scads.lib

import _root_.scala.xml.{Elem,NodeSeq,Text}

import edu.berkeley.cs.comet._

object ReplHelpers {
  type Viewable = {def toHtml: NodeSeq}
  implicit def toViewable(obj: Viewable)(implicit repl: net.liftweb.http.CometActor) = new {
    def view: Unit = repl ! DisplayNodeSeq(obj.toHtml)
  }
}
