package edu.berkeley.cs.scads.model.parser

/**
 * Simple printer that takes in the AST and prints it in a human readable format.
 */
object Printer extends Generator[Tree] {
	protected def generate(elm: Tree)(implicit sb: StringBuilder, indnt: Indentation):Unit = {
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
					indent{
						output("joins:")
						indent{q.joins.foreach(generate(_))}
						output("predicates:")
						indent{q.predicates.foreach(generate(_))}
						output("order:")
						indent{generate(q.order)}
						output("range:")
						indent{generate(q.range)}
					}
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
			case AttributeValue(entity, name) => output("AttributeValue ", entity, ".", name)
			case TrueValue => "TrueValue"
			case FalseValue => "FalseValue"
			case Unordered => output("Unordered")
			case OrderedByField(field, direction) => {
				output("OrderedByFields")
				generate(direction)
				indent{(generate(field))}
			}
			case Ascending => output("Ascending")
			case Descending => output("Descending")
			case Limit(lim, max) => output("Limit: ", lim.toString, ", ", max.toString, " max")
			case Paginate(perPage, max) => output("Paginated: ", perPage.toString, " per page, ", max.toString, " max")
			case Unlimited => output("Unlimited")
		}
	}
}
