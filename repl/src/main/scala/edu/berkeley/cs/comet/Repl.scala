package edu.berkeley.cs {
package comet {

import _root_.scala.actors.Actor
import _root_.scala.xml.{Elem,NodeSeq,Text}
import _root_.net.liftweb.http.{ListenerManager,CometActor,CometListener,SHtml}

import _root_.net.liftweb.util.Helpers
import net.liftweb.http.SHtml
import net.liftweb.http.RenderOut
import net.liftweb.http.js.JsCmds._
import net.liftweb.http.js.JE._
import net.liftweb.http.js.jquery.JqJsCmds._
import java.io._
import Helpers._
import _root_.net.liftweb.common.{Box,Empty,Full}

import scala.xml.XML

import scala.tools.nsc._

case class Exec(cmd: String)
case class Disp(seq: NodeSeq)

class Repl extends CometActor {
	val settings = new Settings
	settings.classpath.value = Thread.currentThread.getContextClassLoader.asInstanceOf[java.net.URLClassLoader].getURLs.toList.mkString(":")
	val stream = new ByteArrayOutputStream
	val writer = new PrintWriter(stream)
	val interp = new Interpreter(settings, writer)
	interp.bind("repl", "net.liftweb.http.CometActor", this)
	interp.interpret("import edu.berkeley.cs.comet._")
	println("Creating interpreter")

  override def lowPriority = {
		case Disp(seq) => partialUpdate(AppendHtml("history", seq))
		case Exec(cmd) => {
			partialUpdate(AppendHtml("history", <p>{cmd}</p>))
			interp.interpret(cmd)
			writer.flush()
			val res = AppendHtml("history", <p>{stream.toString()}</p>)
			stream.reset()
			partialUpdate(res)
		}
  }

	def render: RenderOut = <span></span>

  override lazy val fixedRender: Box[NodeSeq] = {
  	SHtml.ajaxText("", (cmd: String) => {this ! Exec(cmd); SetValById("cmdline", "") }, ("id", "cmdline"))
	}
}}}
