package edu.berkeley.cs.scads.piql.parser

/**
 * Framework for a generator class that takes in a Tree of type InputType and returns a string.
 * You provide the generate function, we take care of indentation and string building.
 */
abstract class Generator[InputType] {
	val indentChar = "  "

	class Indentation {
		var count = 0
	}

	/**
	 * Generate a string based supplied tree
	 */
	def apply(elm: InputType): String = {
		implicit val indnt = new Indentation
		implicit val sb = new StringBuilder
		generate(elm)

		sb.toString
	}

	/**
	 * Increases the indentation level for all outputs that are made during the provided function.
	 * Intended to be nested arbitarily.
	 */
	protected def indent(func: => Unit)(implicit sb: StringBuilder, indnt: Indentation): Unit = {
		indnt.count += 1
		func
		indnt.count -= 1
	}

	/**
	 * Append the concatinated strings provided in parts to the StringBuilder followed by a newline
	 */
	protected def output(parts: String*)(implicit sb: StringBuilder, indnt: Indentation):Unit = {
		(0 to indnt.count).foreach((i) => sb.append(indentChar))
		parts.foreach(sb.append(_))
		sb.append("\n")
	}

  protected def outputBraced(parts: String*)(child: => Unit)(implicit sb: StringBuilder, indnt: Indentation):Unit =
    outputCont(" {\n", "}\n", parts:_*)(child)

  protected def outputParen(parts: String*)(child: => Unit)(implicit sb: StringBuilder, indnt: Indentation):Unit =
    outputCont(" (\n", ");\n", parts:_*)(child)

	protected def outputCont(start: String, end: String, parts: String*)(child: => Unit)(implicit sb: StringBuilder, indnt: Indentation):Unit = {
		(0 to indnt.count).foreach((i) => sb.append(indentChar))
		parts.foreach(sb.append(_))
		sb.append(start)
		indent {
			child
		}
		(0 to indnt.count).foreach((i) => sb.append(indentChar))
		sb.append(end)
	}

	protected def outputPartial(parts: String*)(implicit sb: StringBuilder, indnt: Indentation):Unit = {
		(0 to indnt.count).foreach((i) => sb.append(indentChar))
		parts.foreach(sb.append(_))
	}

	protected def outputPartialCont(parts: String*)(implicit sb: StringBuilder, indnt: Indentation):Unit = {
		parts.foreach(sb.append(_))
	}

	protected def outputPartialEnd()(implicit sb: StringBuilder, indnt: Indentation):Unit = {
    sb.append("\n")
  }

  protected def generate(elm: InputType)(implicit sb: StringBuilder, indnt: Indentation): Unit

  protected def quote(string: String) = "\"" + string.replace("\"", "\\\"") + "\""
}
