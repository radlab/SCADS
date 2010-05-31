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
import net.liftweb.http.js.jquery.JqJsCmds.AppendHtml
import java.io._
import Helpers._
import _root_.net.liftweb.common.{Box,Empty,Full}

import scala.xml.XML

import scala.tools.nsc._
import edu.berkeley.cs.scads.piql.parser._
import edu.berkeley.cs.snippet.PiqlSpec

object InitRepl
case class Exec(cmd: String)
case class Disp(seq: NodeSeq)

class Repl extends CometActor {
	val settings = new Settings
	settings.classpath.value = Thread.currentThread.getContextClassLoader.asInstanceOf[java.net.URLClassLoader].getURLs.toList.mkString(":")
	val stream = new ByteArrayOutputStream
	val writer = new PrintWriter(stream)
	val interp = new Interpreter(settings, writer)
  this ! InitRepl

  override def lowPriority = {
    case InitRepl => {
      outputToConsole(<p>Initalizing REPL</p>)
     	interp.bind("repl", "net.liftweb.http.CometActor", this)
      PiqlSpec.get match {
        case Full(spec) => {
          val code = ScalaGen(spec)
          val unpackaged = code.split("\n").drop(2).mkString("\n")
          interp.interpret(unpackaged)
        }
        case _ =>
      }
      outputToConsole(<p>Ready...</p>)
    }
		case Disp(seq) => partialUpdate(AppendHtml("history", seq))
		case Exec(cmd) => {
			outputToConsole(<p>{cmd}</p>)
			interp.interpret(cmd)
			writer.flush()
			outputToConsole(<p>{stream.toString()}</p>)
			stream.reset()
		}
  }

  private def outputToConsole(text: NodeSeq): Unit = {
    partialUpdate(CmdPair(AppendHtml("history", text), JsRaw("""var objDiv = document.getElementById("history"); objDiv.scrollTop = objDiv.scrollHeight;""")))
  }

	def render: RenderOut = <span></span>

  override lazy val fixedRender: Box[NodeSeq] = {
  	SHtml.ajaxText("", (cmd: String) => {this ! Exec(cmd); SetValueAndFocus("cmdline", "") }, ("id", "cmdline"))
	}
}}}
