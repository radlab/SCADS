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
	protected def indent[A](func: => A)(implicit sb: StringBuilder, indnt: Indentation): A = {
		indnt.count += 1
		val result: A = func
		indnt.count -= 1
    return result
	}

	/**
	 * Append the concatinated strings provided in parts to the StringBuilder followed by a newline
	 */
	protected def output(parts: String*)(implicit sb: StringBuilder, indnt: Indentation):Unit = {
		(0 to indnt.count).foreach((i) => sb.append(indentChar))
		parts.foreach(sb.append(_))
		sb.append("\n")
	}

  protected def outputBraced[A](parts: String*)(child: => A)(implicit sb: StringBuilder, indnt: Indentation):A =
    outputCont(" {\n", "}\n", parts:_*)(child)

  protected def outputParen[A](parts: String*)(child: => A)(implicit sb: StringBuilder, indnt: Indentation):A =
    outputCont(" (\n", ");\n", parts:_*)(child)

	protected def outputCont[A](start: String, end: String, parts: String*)(child: => A)(implicit sb: StringBuilder, indnt: Indentation):A = {
		(0 to indnt.count).foreach((i) => sb.append(indentChar))
		parts.foreach(sb.append(_))
		sb.append(start)
		val result: A = indent {
			child
		}
		(0 to indnt.count).foreach((i) => sb.append(indentChar))
		sb.append(end)
    return result
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

  protected def quote(string: String) = "\"" + string.replaceAll("\"", "\\\"").replaceAll("\n", "\\\\n") + "\""
}
