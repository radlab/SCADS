package edu.berkeley.cs.scads.model.parser

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.combinator.lexical._

class CompileException extends Exception
case class DuplicateEntityException(entityName: String) extends CompileException
case class DuplicateAttributeException(attr: String) extends CompileException

class Lexer extends StdLexical with ImplicitConversions
class ScadsLanguage extends StdTokenParsers with ImplicitConversions {
	type Tokens = Lexer
  	val lexical = new Lexer

	lexical.reserved ++= List("ENTITY", "PRIMARY", "RELATIONSHIP", "FROM", "TO", "ONE", "MANY", "QUERY", "FETCH", "OF", "BY", "WHERE", "AND", "OR", "ORDER", "BY", "LIMIT", "MAX", "PAGINATE", "UNION", "this", "string", "int", "bool", "true", "false")
 	lexical.delimiters ++= List("{", "}", "[", "]", "<", ">", "(", ")", ",", ":", ";", "=", ".")

	def intLiteral: Parser[Int] =
    	accept("int constant", {
      		case lexical.NumericLit(n) if !n.contains(".") && !n.contains("e") && !n.contains("E") && n.exists(_.isDigit) => n.toInt
    	})

	def stringLiteral: Parser[String] =
    	accept("string literal", {
      		case lexical.StringLit(s) => s
    	})

  	def identifier: Parser[String] =
    	accept("identifier", {
      		case lexical.Identifier(s) if !s.contains("-") => s
    	})

	/* Entity Parsing */
	def attrType: Parser[AttributeType] = (
			"string" ^^ ((_) => StringType)
		|	"int" ^^ ((_) => IntegerType)
		|	"bool" ^^ ((_) => BooleanType)
		)

	def attribute: Parser[Attribute] = attrType ~ ident ^^
		{case attrType ~ attrName => new Attribute(attrName, attrType)}

	def primaryKey: Parser[List[String]] = "PRIMARY" ~> "(" ~> repsep(ident, ",") <~")"

	def entity: Parser[Entity] = "ENTITY" ~ ident ~ "{" ~ repsep(attribute, ",") ~ primaryKey ~ "}" ^^
		{case "ENTITY" ~ eName ~ "{" ~ attrs ~ pk ~ "}" => new Entity(eName, attrs, pk)}

	/* Relationship Parsing */

	def cardinality: Parser[Cardinality] = (
			"ONE" ^^ (x => OneCardinality)
		|	intLiteral ^^ ((x:Int) => new FixedCardinality(x))
		|	"MANY" ^^ (x => InfiniteCardinality))

	def relationship: Parser[Relationship] = "RELATIONSHIP" ~ ident ~ "FROM" ~ ident ~ "TO" ~ cardinality ~ ident ^^
		{case "RELATIONSHIP" ~ name ~ "FROM" ~ fromEntity ~ "TO" ~ card ~ toEntity => new Relationship(name, fromEntity, toEntity, card)}

	/* Query Parsing */
	def parameter: Parser[Value] = (
			"[" ~ intLiteral ~ ":" ~ ident ~ "]" ^^ {case "[" ~ ordinal ~ ":" ~ name ~ "]" => new Parameter(name, ordinal)}
		|	"[" ~ "this" ~ "]" ^^ (x => ThisParameter) )

	def field: Parser[Field] = (
			ident ~ "." ~ ident ^^ {case entity ~ "." ~ name => new Field(entity, name)}
		|	ident ^^ ((name:String) => new Field(null, name))
	)

	def value: Parser[Value] = (
			parameter
		|	stringLiteral ^^ ((x) => new StringValue(x))
		|	intLiteral ^^ ((x) => new NumberValue(x.toInt))
		|	"true" ^^ ((x) => TrueValue)
		|	"false" ^^ ((x) => FalseValue)
		|	field)

	def predicate: Parser[Predicate] =
			field ~ "=" ~ value ^^ {case v1 ~ "=" ~ v2 => new EqualityPredicate(v1, v2)}

	def conjunction: Parser[List[Predicate]] = repsep(predicate, "AND")
	def disjunction: Parser[List[List[Predicate]]] = repsep(conjunction, "OR")

	def ordering: Parser[Order] = opt("ORDER" ~> "BY" ~> repsep(field, ",")) ^^
		{
			case Some(fields) => new OrderedByField(fields)
			case None => Unordered
		}


	def limit: Parser[Range] = "LIMIT" ~ value ~ "MAX" ~ intLiteral ~ opt("OFFSET" ~> value) ^^
		{
			case "LIMIT" ~ lim ~ "MAX" ~ max ~ Some(off) => new OffsetLimit(lim, max, off)
			case "LIMIT" ~ lim ~ "MAX" ~ max ~ None => new Limit(lim, max)
		}

	def pagination: Parser[Range] = "PAGINATE" ~ value ~ "MAX" ~ intLiteral ^^
		{case "PAGINATE" ~ perPage ~ "MAX" ~ max => new Paginate(perPage, max)}

	def range: Parser[Range] = opt(limit | pagination) ^^
		{
			case Some(range) => range
			case None => Unlimited
		}

	def where: Parser[List[Predicate]] = opt("WHERE" ~> conjunction) ^^
		{
			case Some(preds) => preds
			case None => List[Predicate]()
		}

	def joinedEntity: Parser[Join] = "OF" ~ ident ~ opt(ident) ~ "BY" ~ ident ^^
		{
			case "OF" ~ entityType ~ None ~ "BY" ~ relationshipName => new Join(entityType, relationshipName, null)
			case "OF" ~ entityType ~ Some(alias) ~ "BY" ~ relationshipName => new Join(entityType, relationshipName, alias)
		}

	def fetch: Parser[Fetch] = "FETCH" ~ ident ~ rep(joinedEntity) ~ where ~ ordering ~ range ^^
		{case "FETCH" ~ entityType ~ joins ~ predicates ~ order ~ limit => new Fetch(entityType, joins, predicates, order, limit)}

	def query: Parser[Query] = "QUERY" ~ ident ~ fetch ^^
		{case "QUERY" ~ name ~ fetch => new Query(name, fetch)}

	def spec: Parser[Spec] = rep(entity) ~ rep(relationship) ~ rep(query) ^^
		{case entities ~ relationships ~ queries => new Spec(entities, relationships, queries)}

	def parse(input: String) =
		phrase(spec)(new lexical.Scanner(input))
}
