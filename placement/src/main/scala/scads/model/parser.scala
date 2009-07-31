package edu.berkeley.cs.scads.model.parser

import scala.util.parsing.combinator._

case class Attribute(name: String, fieldType: String) {
	def objDeclaration(s: StringBuilder) = {
		s.append("object "); s.append(name); s.append(" extends "); s.append(fieldType); s.append(";\n")
	}

	def nameMapping(s: StringBuilder) = {
		s.append("\""); s.append(name); s.append("\" -> "); s.append(name); s.append(",")
	}
}
case class Entity(name: String, attributes: List[Attribute], keys: List[String]) {
	def classDeclaration(s: StringBuilder) = {
		s.append("class "); s.append(name); s.append(" extends Entity()(ScadsEnv) {\n")
		s.append("val namespace = \"tbl_"); s.append(name); s.append("\";\n")
		s.append("val version = new IntegerVersion();\n")
		attributes.foreach(_.objDeclaration(s))
		s.append("val attributes = Map("); attributes.foreach(_.nameMapping(s)); s.append(");\n")
		s.append("val indexes = new scala.collection.mutable.LinkedList[Index](null, null);\n")
		s.append("val primaryKey = ")
		if(keys.size > 1) {
			s.append("new CompositeKey("); s.append(keys.mkString("", ", ", "")); s.append(")\n;")
		}
		else {
			s.append(keys(0)); s.append(";\n")
		}
		s.append("}\n")
	}
}


case class Spec(entities: List[Entity]) {
	def generateSpec: String = {
		val s = new StringBuilder
		imports(s)
		env(s)
		entities.foreach(_.classDeclaration(s))

		s.toString
	}

	private def imports(s: StringBuilder) = s.append("import edu.berkeley.cs.scads.model._\n\n")
	private def env(s: StringBuilder) = s.append("object ScadsEnv extends Environment;\n")
}

class ScadsLanguage extends JavaTokenParsers {
	def attrType: Parser[String] = (
			"string" ^^ ((_) => "StringField")
		|	"int" ^^ ((_) => "IntegerField")
		)

	def attribute: Parser[Attribute] = attrType ~ ident ^^
		{case attrType ~ attrName => new Attribute(attrName, attrType)}

	def primaryKey: Parser[List[String]] = "PRIMARY" ~> "(" ~> repsep(ident, ",") <~")"

	def entity: Parser[Entity] = "ENTITY" ~ ident ~ "{" ~ repsep(attribute, ",") ~ primaryKey ~ "}" ^^
		{case "ENTITY" ~ eName ~ "{" ~ attrs ~ pk ~ "}" => new Entity(eName, attrs, pk)}

	def spec: Parser[Spec] = rep(entity) ^^
		{case entities: List[Entity] => new Spec(entities)}
}
