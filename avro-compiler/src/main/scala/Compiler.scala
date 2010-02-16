package edu.berkeley.cs.scads.avro.compiler

import java.util.{ArrayList, Collection => JCollection}
import java.io.{File,FileOutputStream,PrintWriter}
import org.apache.avro.Schema
import org.apache.avro.Schema.{Type,Field}
import org.apache.avro.Schema.Field.Order
import org.apache.avro.Protocol

import scala.collection.mutable.HashMap
import scala.collection.jcl.Conversions 

import org.apache.log4j.Logger

object CompilerMain {
    def main(args: Array[String]):Unit = {
        val jsonFileName = args(0)
        val jsonFile = new File(jsonFileName)
        val protocol = Protocol.parse(jsonFile)
        val output = new Compiler(protocol).compile
        if (args.length > 1) {
            val outputFile = new File(args(1))
            val outputStream = new FileOutputStream(outputFile)
            val printWriter = new PrintWriter(outputStream)
            printWriter.println(output)
            printWriter.flush
            printWriter.close
        } else 
            println(output)
    }
}

case class UnsupportedFeatureException(val msg: String) extends RuntimeException

class Compiler(val protocol: Protocol) {
    private val logger = Logger.getLogger("Compiler")

    private val name = protocol.getName
    private val namespace = protocol.getNamespace
    private val schemas = protocol.getTypes 

    // base class for classes we will generate in compiler
    abstract class AvroClass(val className: String)

    // -- records and union interfaces -- //
    // this class is for all record objects defined in JSON
    case class AvroRecordClass(val schema: Schema) extends AvroClass(schema.getName)
    // this class is for all the union interfaces that need to be defined
    case class AvroUnionInterfaceClass(_className: String) extends AvroClass(_className)

    // --- special cases --- //
    case class AvroFixedClass(val schema: Schema) extends AvroClass(schema.getName)
    case class AvroEnumClass(val schema: Schema) extends AvroClass(schema.getName)

    // -- lists and maps contained within unions -- //
    // this class is for arrays that live within unions,
    case class AvroUnionListClass(val schema: Schema) extends AvroPrimitiveClass("UnionList_" + schema.getElementType.getName, "List[" + getAvroJavaClassName(schema.getElementType, null, null) + "]", "AvroUnionListClass") {
        schema.getElementType.getType match {
            case Type.ARRAY => throw new UnsupportedFeatureException("Cannot support arrays of arrays")
            case Type.MAP   => throw new UnsupportedFeatureException("Cannot support arrays of maps")
            case Type.UNION => throw new UnsupportedFeatureException("Cannot support arrays of unions")
            case _ => /* no op */
        }
    }
    // this class is for maps that live within unions,
    case class AvroUnionMapClass(val schema: Schema) extends AvroPrimitiveClass("UnionMap_" + schema.getValueType.getName, "scala.collection.Map[String, " +getAvroJavaClassName(schema.getValueType, null, null) + "]", "AvroUnionMapClass") {
        schema.getValueType.getType match {
            case Type.ARRAY => throw new UnsupportedFeatureException("Cannot support maps of arrays")
            case Type.MAP   => throw new UnsupportedFeatureException("Cannot support maps of maps")
            case Type.UNION => throw new UnsupportedFeatureException("Cannot support maps of unions")
            case _ => /* no op */
        }
    }


    // --- primitives contained within unions --- //
    abstract class AvroPrimitiveClass(_className: String, val primitiveClassName: String, val wrapperClassName: String) extends AvroClass(_className) 
    case object AvroIntClass extends AvroPrimitiveClass("AvroInt", "Int", "AvroIntClass")
    case object AvroLongClass extends AvroPrimitiveClass("AvroLong", "Long", "AvroLongClass")
    case object AvroFloatClass extends AvroPrimitiveClass("AvroFloat", "Float", "AvroFloatClass")
    case object AvroDoubleClass extends AvroPrimitiveClass("AvroDouble", "Double", "AvroDoubleClass")
    case object AvroBooleanClass extends AvroPrimitiveClass("AvroBoolean", "Boolean", "AvroBooleanClass")
    case object AvroStringClass extends AvroPrimitiveClass("AvroString", "org.apache.avro.util.Utf8", "AvroStringClass")
    case object AvroBytesClass extends AvroPrimitiveClass("AvroBytes", "java.nio.ByteBuffer", "AvroBytesClass")
    

    // Maps Record name (Class name) to a list of Union Interfaces
    // that it will have to implement. The keys in this map is the source
    // for all the classes that will be generated by the compiler
    private val classMap = new HashMap[AvroClass, List[AvroUnionInterfaceClass]]

    private val generator = new AvroCompilerGenerator
    private val conversionGenerator = new ConversionGenerator

    class ConversionGenerator extends Generator[AvroPrimitiveClass] {
        protected def generate(elm: AvroPrimitiveClass)(implicit sb: StringBuilder, indnt: Indentation):Unit = {
            indent {
                classMap.get(elm).get.foreach( unionIfaceClass => {
                    output(
                        "implicit def " + elm.className + "2" + unionIfaceClass.className + 
                        "(primitive: " + elm.primitiveClassName + "): " + unionIfaceClass.className + " = {")
                    indent {
                        output("new " + elm.className + "(primitive)")    
                    }
                    output("}")
                })
            }
        }
    }

    private def escapeQuotes(str: String):String = { str.replaceAll("\"","\\\\\"") }

    class AvroCompilerGenerator extends Generator[AvroClass] {
        protected def generate(elm: AvroClass)(implicit sb: StringBuilder, indnt: Indentation):Unit = {
            elm match {
                case union: AvroUnionInterfaceClass => {
                    output("sealed trait " + union.className + " extends UnionInterface")
                }
                case primitive: AvroPrimitiveClass => {
                    val str1 = "case class " + primitive.className + "(_value: " + primitive.primitiveClassName + ") extends PrimitiveWrapper["
                    val str2 = primitive.primitiveClassName
                    val str3 = "](_value)"
                    var str4 = ""
                    if (!classMap.get(primitive).get.isEmpty) {
                        str4 += " with "
                        str4 += classMap.get(primitive).get.map(_.className.trim).mkString(""," with ","")
                    }
                    output(str1+str2+str3+str4)
                }
                case rec: AvroRecordClass => {
                    output("// minor optimization so that each new instance does not have to reparse the schema //")
                    output("object " + rec.className + " {") 
                    indent {
                        output("val SCHEMA$ = org.apache.avro.Schema.parse(\"" + escapeQuotes(rec.schema.toString) + "\")")
                    }
                    output("}")

                    outputPartial("case class " + rec.className)
                    sb.append("(")
                    var count = 0
                    val numFields = rec.schema.getFields.size
                    iterateRecordFields(rec.schema, (fieldName, field) => {
                        val fieldSchema = field.schema
                        fieldSchema.getType match { 
                            case Type.ARRAY => 
                                sb.append("var " + fieldName + ":List[" + getAvroJavaClassName(fieldSchema.getElementType, fieldName, fieldSchema) + "]")
                            case Type.BOOLEAN => 
                                sb.append("var " + fieldName + ":Boolean")
                            case Type.BYTES =>
                                sb.append("var " + fieldName + ":java.nio.ByteBuffer")
                            case Type.DOUBLE =>
                                sb.append("var " + fieldName + ":Double")
                            case Type.ENUM => 
                                throw new UnsupportedFeatureException("enums not supported at this time")
                            case Type.FLOAT =>
                                sb.append("var " + fieldName + ":Float")
                            case Type.INT => 
                                sb.append("var " + fieldName + ":Int")
                            case Type.LONG => 
                                sb.append("var " + fieldName + ":Long")
                            case Type.MAP   => 
                                sb.append("var " + fieldName + ":scala.collection.Map[String, " + getAvroJavaClassName(fieldSchema.getValueType, fieldName, fieldSchema) + "]")
                            case Type.NULL =>
                                sb.append("var " + fieldName + ":Void")
                            case Type.RECORD => 
                                sb.append("var " + fieldName + ":" + fieldSchema.getName)
                            case Type.STRING => 
                                sb.append("var " + fieldName + ":org.apache.avro.util.Utf8")
                            case Type.UNION => 
                                var unionInterfaceName = rec.schema.getName + "_" + fieldName + "_Iface"
                                sb.append("var " + fieldName + ":" + unionInterfaceName)
                        }
                        if (count != numFields-1) sb.append(", ")
                        count += 1
                    })
                    sb.append(") ")
                    sb.append("extends org.apache.avro.specific.SpecificRecordBase")
                    outputPartial(" with org.apache.avro.specific.SpecificRecord")
                    if (!classMap.get(rec).get.isEmpty)
                        outputPartial(" with ")
                    outputPartial(classMap.get(rec).get.map(_.className.trim).mkString(""," with ",""))
                    output(" {")
                    indent {
                        count = 0 
                        if (numFields > 0) {
                            outputPartial("def this() = this(")
                            iterateRecordFields(rec.schema, (fieldName, field) => {
                                val fieldSchema = field.schema
                                fieldSchema.getType match { 
                                    case Type.ARRAY => sb.append("null")
                                    case Type.BOOLEAN => sb.append("false")
                                    case Type.BYTES => sb.append("null")
                                    case Type.DOUBLE => sb.append("0.0")
                                    case Type.ENUM => throw new UnsupportedFeatureException("enums not supported at this time")
                                    case Type.FLOAT => sb.append("0.0")
                                    case Type.INT => sb.append("0")
                                    case Type.LONG => sb.append("0")
                                    case Type.MAP   => sb.append("null")
                                    case Type.NULL => sb.append("null")
                                    case Type.RECORD => sb.append("null")
                                    case Type.STRING => sb.append("null")
                                    case Type.UNION => sb.append("null")
                                }
                                if (count != numFields-1) sb.append(", ")
                                count += 1
                            })
                            sb.append(")\n") 
                        }


                        output("private def wrapUnion(value: Object, fieldName:String):UnionInterface = value match {")
                        indent {
                            output("case null => null")

                            classMap.keys.filter(_.isInstanceOf[AvroPrimitiveClass]).foreach( clazz => {
                                clazz match {
                                    case AvroIntClass => 
                                        output("case int:java.lang.Integer => new AvroInt(int.intValue)")
                                    case AvroLongClass =>
                                        output("case long:java.lang.Long => new AvroLong(long.longValue)")
                                    case AvroDoubleClass => 
                                        output("case double:java.lang.Double => new AvroDouble(double.doubleValue)")
                                    case AvroFloatClass => 
                                        output("case float:java.lang.Float => new AvroFloat(float.floatValue)")
                                    case AvroBooleanClass => 
                                        output("case boolean:java.lang.Boolean => new AvroBoolean(boolean.booleanValue)")
                                    case AvroStringClass => 
                                        output("case string:org.apache.avro.util.Utf8 => new AvroString(string)")
                                    case AvroBytesClass => 
                                        output("case bytes:java.nio.ByteBuffer => new AvroBytes(bytes)")
                                    case _ => /* no op */ 
                                }
                            })
                            
                            output("case _ => {")
                            indent {
                                output("fieldName match {")
                                iterateRecordFields(rec.schema, (fieldName, field) => {
                                    if (field.schema.getType == Type.UNION) {
                                        indent {
                                            output("case \"" + fieldName + "\" => value match {")
                                            indent {
                                                iterateUnionTypes(field.schema, schema => {
                                                    schema.getType match { 
                                                        case Type.ARRAY => 
                                                            output("case array: org.apache.avro.generic.GenericArray[_]" +  
                                                                    " => new UnionList_" + schema.getElementType.getName + 
                                                                    "(ScalaLib.convertGenericArrayToList(array))")
                                                        case Type.MAP =>
                                                            output("case map: java.util.Map[String,_]" + 
                                                                    " => new UnionMap_" + schema.getValueType.getName + 
                                                                    "(ScalaLib.convertJMapToMap(map.asInstanceOf[java.util.Map[String," +
                                                                    getAvroJavaClassName(schema.getValueType, fieldName, rec.schema) + "]]))")
                                                        case Type.RECORD => 
                                                            output("case record: " + schema.getName + " => record")
                                                        case _ => /* no op */
                                                    }
                                                })
                                                output("case _ => throw new org.apache.avro.AvroRuntimeException(\"here\")") 
                                            }
                                            output("}")
                                        }
                                    }
                                })
                                indent { output("case _ => throw new org.apache.avro.AvroRuntimeException(\"here\")") }
                                output("}")
                            }
                            output("}")
                        }
                        output("}")
                        
                        output("private def getSchemaForFieldName(fieldName: String):org.apache.avro.Schema = {")
                        indent {
                            output("getSchema.getFields.get(fieldName).schema")
                        }
                        output("}")

                        output("val SCHEMA$ = getSchema")

                        output("override def getSchema():org.apache.avro.Schema = " + rec.className + ".SCHEMA$")
                        
                        output("override def get(field$: Int):Object = {")
                        indent {
                            output("field$ match {")
                            indent {
                                fieldsInOrder(rec.schema).foreach(field => {
                                    val str1 = "case " + field._2.pos + " => "
                                    field._2.schema.getType match {
                                        case Type.ARRAY => output(str1 + "ScalaLib.convertListToGenericArray(" + field._1 + ", getSchemaForFieldName(\"" + field._1 +"\"))")
                                        case Type.MAP   => output(str1 + "ScalaLib.convertMapToJMap(" + field._1 + ")")
                                        case Type.UNION => output(str1 + "ScalaLib.unwrapUnion(" + field._1 + ", getSchemaForFieldName(\"" + field._1 +"\"))") 
                                        case Type.INT => output(str1+field._1+".asInstanceOf[java.lang.Integer]")
                                        case Type.LONG => output(str1+field._1+".asInstanceOf[java.lang.Long]")
                                        case Type.DOUBLE => output(str1+field._1+".asInstanceOf[java.lang.Double]")
                                        case Type.FLOAT => output(str1+field._1+".asInstanceOf[java.lang.Float]")
                                        case Type.BOOLEAN => output(str1+field._1+".asInstanceOf[java.lang.Boolean]")
                                        case _ => output(str1 + field._1) 
                                    }
                                })
                                output("case _ => throw new org.apache.avro.AvroRuntimeException(\"Bad index\")")
                            }
                            output("}")
                        }
                        output("}")

                        output("override def set(field$: Int, value$: Object):Unit = {")
                        indent {
                            output("field$ match {")
                            indent {
                                fieldsInOrder(rec.schema).foreach(field => {
                                    val str1 = "case " + field._2.pos + " => " + field._1 + " = "
                                    val str2 = field._2.schema.getType match {
                                        case Type.UNION => 
                                            "wrapUnion(value$,\"" + field._1 + "\").asInstanceOf[" + rec.schema.getName + "_" + field._1 + "_Iface]"
                                        case Type.ARRAY => 
                                            "ScalaLib.convertGenericArrayToList(value$.asInstanceOf[org.apache.avro.generic.GenericArray[" +  
                                            getAvroJavaClassName(field._2.schema.getElementType, field._1, rec.schema) + "]])"
                                        case Type.MAP =>
                                            "ScalaLib.convertJMapToMap(value$.asInstanceOf[java.util.Map[String, " +  
                                            getAvroJavaClassName(field._2.schema.getValueType, field._1, rec.schema) + "]])"
                                        case Type.RECORD =>
                                            "value$.asInstanceOf[" + field._2.schema.getName + "]"
                                        case Type.ENUM => 
                                            "value$.asInstanceOf[" + field._2.schema.getName + "]"
                                        case _ =>
                                            "value$.asInstanceOf[" + 
                                            getAvroJavaClassName(field._2.schema, field._1, rec.schema) + "]"
                                    }
                                    output(str1 + str2)
                                })
                                output("case _ => throw new org.apache.avro.AvroRuntimeException(\"Bad index\")")
                            }
                            output("}")
                        }
                        output("}")
                    }
                    output("}")
                }
            } 
        }
    }

    private def getAvroJavaClassName(schema: Schema, fieldName: String, record: Schema):String = schema.getType match {
        case Type.ARRAY => "org.apache.avro.generic.GenericArray[" + getAvroJavaClassName(schema.getElementType, null, record) + "]"
        case Type.BOOLEAN => "Boolean"
        case Type.BYTES => "java.nio.ByteBuffer"
        case Type.DOUBLE => "Double"
        case Type.ENUM => schema.getName 
        case Type.FLOAT => "Float"
        case Type.INT => "Int"
        case Type.LONG => "Long"
        case Type.MAP   => "java.util.Map[String, " + getAvroJavaClassName(schema.getValueType, null, record) + "]"
        case Type.NULL => "java.lang.Void"
        case Type.RECORD => schema.getName 
        case Type.STRING => "org.apache.avro.util.Utf8"
        case Type.UNION => 
            val str1 = record match {
                case null => "anonRecord"
                case _    => record.getName
            }
            val str2 = fieldName match {
                case null => "anonField"
                case _    => fieldName
            }
            str1 + "_" + str2 + "_Iface"
    }

    private implicit def mkList[T](collection: JCollection[T]):List[T] = {
        val arrayList = new ArrayList[T]
        arrayList.addAll(collection)
        Conversions.convertList(arrayList).toList
    }

    private def issueWarning(warning: String) = {
        logger.warn(warning)
    }

    private def validate = {
        if (!protocol.getMessages.isEmpty)
            throw new UnsupportedFeatureException("Message constructs not yet supported")
    }

    def compile:String = {
        // Step 0: Validate
        validate

        // Step 1: Add all records to the class map
        schemas.foreach(schema => {
            schema.getType match {
                case Type.RECORD => 
                    classMap += AvroRecordClass(schema) -> List[AvroUnionInterfaceClass]()
            }
        })

        // Step 2: Resolve all union types
        schemas.foreach(schema => {
            schema.getType match {
                case Type.RECORD => preProcessRecord(schema)
                case _ => throw new UnsupportedFeatureException("Cannot handle: " + schema.getType) 
            } 
        })

        println(classMap)

        val sb = new StringBuilder
        sb.append("/*\n")
        sb.append(" * Auto generated scala avro file\n")
        sb.append(" * avro scala compiler v1.0\n")
        sb.append(" */\n\n")
        sb.append("package ")
        sb.append(namespace)
        sb.append("\n\n")

        sb.append("// Runtime dependency imports //\n")
        sb.append("import edu.berkeley.cs.scads.avro.compiler.{UnionInterface, PrimitiveWrapper, ScalaLib}\n\n")



        sb.append("// Union Interfaces //\n")
        classMap.keys.filter(_.isInstanceOf[AvroUnionInterfaceClass]).foreach(c => sb.append(generator(c))) 
        sb.append("\n\n")

        if (!classMap.keys.filter(_.isInstanceOf[AvroPrimitiveClass]).toList.isEmpty) {
            sb.append("// Primitive Class Wrappers //\n")
            classMap.keys.filter(_.isInstanceOf[AvroPrimitiveClass]).foreach(c => sb.append(generator(c)))
            sb.append("\n\n")
        }

        /*
        if (!classMap.keys.filter(_.isInstanceOf[AvroUnionListClass]).toList.isEmpty) {
            sb.append("// Union List Class Wrappers //\n")
            classMap.keys.filter(_.isInstanceOf[AvroUnionListClass]).foreach(c => sb.append(generator(c)))
            sb.append("\n\n")
        }
        */

        sb.append("// Protocol //\n")
        sb.append("object " + name + " {\n")
        sb.append("    val PROTOCOL = org.apache.avro.Protocol.parse(\"")
        sb.append(escapeQuotes(protocol.toString))
        sb.append("\")\n")
        sb.append("// Implicit Conversion Helpers //\n")
        sb.append("object AvroConversions {\n")
        classMap.keys.filter(_.isInstanceOf[AvroPrimitiveClass]).foreach(c => sb.append(conversionGenerator(c.asInstanceOf[AvroPrimitiveClass])))
        sb.append("}\n\n")
        sb.append("}\n\n")

        sb.append("// Record Classes //\n")
        classMap.keys.filter(_.isInstanceOf[AvroRecordClass]).foreach(c => sb.append(generator(c)))

        sb.toString
    }

    private def fieldsInOrder(schema: Schema):List[(String,Field)] = {
        assert( schema.getType == Type.RECORD )
        Conversions.convertMap(schema.getFields).map(e => (e._1,e._2)).toList.sort(_._2.pos<_._2.pos)
    }

    private def iterateRecordFields(schema: Schema, closure: (String, Field) => Unit) = {
        assert( schema.getType == Type.RECORD )
        Conversions.convertMap(schema.getFields).foreach(entry => {
            val fieldName = entry._1 
            val field = entry._2 
            closure(fieldName, field)
        })
    }

    private def iterateUnionTypes(schema: Schema, closure: (Schema) => Unit) = {
        assert( schema.getType == Type.UNION )
        Conversions.convertList(schema.getTypes).toList.foreach(closure(_))
    }

    private def processRecord(schema: Schema) = {


    }

    private def preProcessRecord(schema: Schema) = {
        iterateRecordFields(schema, (fieldName, field) => {
            if (field.order != Order.IGNORE)
                issueWarning("Sort Order " + field.order + " will be ignored")
            val fieldSchema = field.schema
            if (fieldSchema.getType == Type.UNION) {
                val unionInterfaceName = schema.getName + "_" + fieldName + "_Iface"
                val unionInterfaceClass = AvroUnionInterfaceClass(unionInterfaceName)
                classMap +=  unionInterfaceClass -> List[AvroUnionInterfaceClass]()
                iterateUnionTypes(fieldSchema, (unionSchema) => {
                    unionSchema.getType match {
                        case Type.ARRAY  => 
                            addInterface(AvroUnionListClass(unionSchema), unionInterfaceClass)
                        case Type.BOOLEAN =>
                            addInterface(AvroBooleanClass, unionInterfaceClass)
                        case Type.BYTES  =>
                            addInterface(AvroBytesClass, unionInterfaceClass)
                        case Type.ENUM =>
                            throw new UnsupportedFeatureException("Enums not supported")
                        case Type.DOUBLE  =>
                            addInterface(AvroDoubleClass, unionInterfaceClass)
                        case Type.FIXED  =>
                            throw new UnsupportedFeatureException("FIXED not supported")
                        case Type.FLOAT  =>
                            addInterface(AvroFloatClass, unionInterfaceClass)
                        case Type.INT  =>
                            addInterface(AvroIntClass, unionInterfaceClass)
                        case Type.LONG  =>
                            addInterface(AvroLongClass, unionInterfaceClass)
                        case Type.MAP  =>
                            addInterface(AvroUnionMapClass(unionSchema), unionInterfaceClass)
                        case Type.RECORD => 
                            addInterface(AvroRecordClass(unionSchema), unionInterfaceClass)
                        case Type.STRING =>  
                            addInterface(AvroStringClass, unionInterfaceClass)
                        case Type.UNION => 
                            throw new UnsupportedFeatureException("Cannot nest UNIONS yet")
                        case Type.NULL => { /* no op */ }
                    }
                }) 
            }
        })
    }

    private def addInterface(avroClass: AvroClass, avroIface: AvroUnionInterfaceClass) = {
        val list = classMap.get(avroClass).getOrElse(List[AvroUnionInterfaceClass]()) ::: List(avroIface)
        classMap += avroClass -> list
   }

}
