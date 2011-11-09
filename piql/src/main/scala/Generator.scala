package edu.berkeley.cs.scads.piql
package viz

import java.io.{File, FileWriter}

protected class Indentation {
  var count = 0
}


trait StringGenerator extends Generator[String] {
  val indnt = new Indentation
  val sb = new StringBuilder

  def append(s: String): Unit = append(s)

  def finish(): String = sb.toString
}

trait FileGenerator extends Generator[File] {
  val file: File
  val fileWriter = new FileWriter(file)

  def append(s: String) = fileWriter.write(s)
  def finish(): File = {
    fileWriter.close
    file
  }
}

/**
 * Framework for a generator class that takes in a Tree of type InputType and returns a string.
 * You provide the generate function, we take care of indentation and string building.
 */
abstract class Generator[OutputType] {
  val indentChar = "  "
  var indent: Int = 0

  def append(s: String): Unit

  def finish(): OutputType

  /**
   * Increases the indentation level for all outputs that are made during the provided function.
   * Intended to be nested arbitarily.
   */
  protected def indent[A](func: => A): A = {
    indent += 1
    val result: A = func
    indent -= 1
    return result
  }

  /**
   * Append the concatinated strings provided in parts to the StringBuilder followed by a newline
   */
  protected def output(parts: String*): Unit = {
    (0 to indent).foreach((i) => append(indentChar))
    parts.foreach(append(_))
    append("\n")
  }

  protected def outputBraced[A](parts: String*)(child: => A): A =
    outputCont(" {\n", "}\n", parts: _*)(child)

  protected def outputParen[A](parts: String*)(child: => A): A =
    outputCont(" (\n", ");\n", parts: _*)(child)

  protected def outputCont[A](start: String, end: String, parts: String*)(child: => A): A = {
    (0 to indent).foreach((i) => append(indentChar))
    parts.foreach(append(_))
    append(start)
    val result: A = indent {
      child
    }
    (0 to indent).foreach((i) => append(indentChar))
    append(end)
    return result
  }

  protected def outputPartial(parts: String*): Unit = {
    (0 to indent).foreach((i) => append(indentChar))
    parts.foreach(append(_))
  }

  protected def outputPartialCont(parts: String*): Unit = {
    parts.foreach(append(_))
  }

  protected def outputPartialEnd(): Unit = {
    append("\n")
  }

  protected def quote(string: String) = "\"" + string.replaceAll("\"", "\\\"").replaceAll("\n", "\\\\n") + "\""
}