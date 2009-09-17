package edu.berkeley.cs.scads.model.parser

abstract class Generator {
	val indentChar = " "

	class Indentation {
		var count = 0
	}

	def apply(elm: Tree): String = {
		implicit val indnt = new Indentation
		implicit val sb = new StringBuilder
		generate(elm)

		sb.toString
	}

	def indent(func: => Unit)(implicit sb: StringBuilder, indnt: Indentation): Unit = {
		indnt.count += 1
		func
		indnt.count -= 1
	}

	def output(parts: String*)(implicit sb: StringBuilder, indnt: Indentation):Unit = {
		(0 to indnt.count).foreach((i) => sb.append(indentChar))
		parts.foreach(sb.append(_))
		sb.append("\n")
	}

	def generate(elm: Tree)(implicit sb: StringBuilder, indnt: Indentation): Unit
}

object Printer extends Generator {
	def generate(elm: Tree)(implicit sb: StringBuilder, indnt: Indentation) = {
		elm match {
			case s: Spec => {
				output("SCADS SPEC:")
				indent{
					output("entities:")
					indent { s.entities.foreach(generate(_))}
					output("relationships:")
					indent { s.relationships.foreach(generate(_)) }
					output("queries:")
					indent { s.queries.foreach(generate(_)) }
				}
			}
			Range
			case r: Relationship => {
				output("Relationship ", r.name, " from ", r.from, " to ", r.to, " of cardinality:")
				indent{generate(r.cardinality)}
			}
			case OneCardinality => output("OneCardinality")
			case FixedCardinality(n) => output("FixedCardinality: max = ", n.toString)
			case InfiniteCardinality => output("InfiniteCardinality")
			case e: Entity => {
				output("Entity ", e.name)
				indent {
					output("attributes:")
					indent{e.attributes.foreach(generate(_))}
					output("primary keys:")
					indent {e.keys.foreach(output(_))}
				}
			}
			case a: Attribute => {
				output("Attr name: ", a.name, ", ", "type: ", a.attrType.toString)
			}
			case q: Query => {
				output("Query ", q.name)
				indent {
					output("Fetches:")
					indent{generate(q.fetch)}
				}
			}
			case f: Fetch => {
				indent{
					output("joins:")
					indent{f.joins.foreach(generate(_))}
					output("predicates:")
					indent{f.predicates.foreach(generate(_))}
					output("order:")
					indent{generate(f.order)}
					output("range:")
					indent{generate(f.range)}
				}
			}
			case e: Join => {
				output("Join ", e.entity, " relationship ", e.relationship)
			}
			case ep: EqualityPredicate => {
				output("EqualityPredicate")
				indent{
					generate(ep.op1)
					generate(ep.op2)
				}
			}
			case Parameter(name, ordinal) => output("Parameter ", name, ", ordinal ", ordinal.toString)
			case ThisParameter => output("ThisParameter")
			case StringValue(value) =>  output("String Value \"", value, "\"")
			case NumberValue(num) => output("Number Value ", num.toString)
			case Field(entity, name) => output("Field ", entity, ".", name)
			case TrueValue => "TrueValue"
			case FalseValue => "FalseValue"
			case Unordered => output("Unordered")
			case OrderedByField(fields) => {
				output("OrderedByFields")
				indent{fields.foreach(generate(_))}
			}
			case Limit(lim, max) => output("Limit: ", lim.toString, ", ", max.toString, " max")
			case OffsetLimit(lim, max, offset) => output("OffsetLimit: limit ", lim.toString, ", max ", max.toString, ", offset ", offset.toString)
			case Paginate(perPage, max) => output("Paginated: ", perPage.toString, " per page, ", max.toString, " max")
			case Unlimited => output("Unlimited")
		}
	}
}
