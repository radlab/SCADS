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
case class ExecuteScala(scalaCmd: String)
case class DisplayNodeSeq(seq: NodeSeq)

class Repl extends CometActor {
	private val settings = new Settings
	settings.classpath.value = Thread.currentThread.getContextClassLoader.asInstanceOf[java.net.URLClassLoader].getURLs.toList.mkString(":")
	private val stream = new ByteArrayOutputStream
	private val writer = new PrintWriter(stream)
	private var interpreter: Interpreter = null

  this ! InitRepl

  override def lowPriority = {
    case InitRepl => {
      outputToConsole(<p>Initalizing REPL</p>)
      interpreter = new Interpreter(settings, writer)
     	interpreter.bind("repl", "net.liftweb.http.CometActor", this)
      PiqlSpec.get match {
        case Full(spec) => {
          val code = ScalaGen(spec)
          val unpackaged = code.split("\n").drop(2).mkString("\n")
          interpret(unpackaged)
        }
        case _ => outputToConsole("No PIQL Spec Available for compilation")
      }
      outputToConsole(<p>Ready...</p>)
    }
		case DisplayNodeSeq(seq) => outputToConsole(seq)
		case ExecuteScala(cmd) => {
			outputToConsole(<p>{cmd}</p>)
      interpret(cmd)
    }
  }

  private def interpret(scalaCmd: String) = {
    interpreter.interpret(scalaCmd)
    flushConsole()
  }

  private def flushConsole() = {
    writer.flush()
    val result = stream.toString
    if(result.length > 0)
      outputToConsole(<p>{result}</p>)
    stream.reset()
  }


  private def outputToConsole(text: NodeSeq): Unit = {
    partialUpdate(CmdPair(AppendHtml("history", text), JsRaw("""var objDiv = document.getElementById("history"); objDiv.scrollTop = objDiv.scrollHeight;""")))
  }

	def render: RenderOut = <span></span>

  override lazy val fixedRender: Box[NodeSeq] = {
  	SHtml.ajaxText("", (cmd: String) => {this ! ExecuteScala(cmd); SetValueAndFocus("cmdline", "") }, ("id", "cmdline"))
	}
}}}
