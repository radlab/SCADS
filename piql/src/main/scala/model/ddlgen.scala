package edu.berkeley.cs.scads.piql.parser

import scala.collection.jcl.Conversions._
import org.apache.log4j.Logger

import org.apache.avro.Schema
import org.apache.avro.Schema.Type


object DDLGen extends Generator[BoundSpec] {
	protected def generate(spec: BoundSpec)(implicit sb: StringBuilder, indnt: Indentation): Unit = {
    spec.entities.foreach {
      case (entityName, entity) => {
        outputParen("CREATE TABLE ", entityName) {
          def outputFields(r: Schema, prefix: String): Unit = {
            r.getFields.map(f => f.schema().getType match {
              case Type.STRING => output(prefix, f.name, " VARCHAR(255),")
              case Type.BOOLEAN =>output(prefix, f.name, " INT(1),")
              case Type.INT =>output(prefix, f.name, " INT(10),")
              case Type.RECORD => outputFields(f.schema, prefix + f.name)
            })
          }
          outputFields(entity.keySchema, "")
          outputFields(entity.valueSchema, "")

          def keyFields(r: Schema, prefix: String): Seq[String] = {
            r.getFields.flatMap(f => f.schema().getType match {
              case Type.RECORD => keyFields(f.schema, prefix + f.name)
              case _ => List(prefix + f.name)
            })
          }
          output("PRIMARY KEY", keyFields(entity.keySchema, "").map("`" + _ + "`").mkString("(", ",", ")"))
        }
      }
    }
  }
}
