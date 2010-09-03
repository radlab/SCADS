package com.googlecode.avro
package util

case class BuilderSet(val name: String, val prefix: String, val entries: List[BuilderEntry])

sealed abstract class BuilderEntry(val coll: String) {
  def collTypeParam: Option[String]
  def elemTypeParam: String
  def builderTypeParam: Option[String]
  def implicitParams: Option[String]
}

case class SpecificBuilderEntry(_coll: String, val elem0: String)
  extends BuilderEntry(_coll) {
  def collTypeParam = None
  def elemTypeParam = elem0
  def builderTypeParam = None
  def implicitParams = None
}

case class BuilderEntry1(_coll: String, val elem0: String)
  extends BuilderEntry(_coll) {
  def collTypeParam = Some(elem0)
  def elemTypeParam = elem0
  def builderTypeParam = Some(elem0)
  def implicitParams = None
}

case class BuilderEntry1Implicit(_coll: String, val elem0: String)
  extends BuilderEntry(_coll) {
  def collTypeParam = Some(elem0)
  def elemTypeParam = elem0
  def builderTypeParam = Some(elem0)
  def implicitParams = Some("ord: scala.math.Ordering[%s]".format(elem0))
}

case class BuilderEntry2(_coll: String, val elem0: String, val elem1: String)
  extends BuilderEntry(_coll) {
  def collTypeParam = Some(List(elem0, elem1).mkString(","))
  def elemTypeParam = "(" + collTypeParam.get + ")"
  def builderTypeParam = Some(List(elem0, elem1).mkString(","))
  def implicitParams = None
}

case class BuilderEntry2ImplicitKey(_coll: String, val elem0: String, val elem1: String)
  extends BuilderEntry(_coll) {
  def collTypeParam = Some(List(elem0, elem1).mkString(","))
  def elemTypeParam = "(" + collTypeParam.get + ")"
  def builderTypeParam = Some(List(elem0, elem1).mkString(","))
  def implicitParams = Some("ord: scala.math.Ordering[%s]".format(elem0))
}


object PrintBuilders {
  val `package` = "com.googlecode.avro.runtime"

  val builders = List(
    BuilderSet("HasCollectionBuilders", "scala.collection",
      List(
        SpecificBuilderEntry("BitSet", "Int"),
        BuilderEntry1("IndexedSeq", "E"),
        BuilderEntry1("Iterable", "E"),
        BuilderEntry1("LinearSeq", "E"),
        BuilderEntry2("Map", "A", "B"),
        BuilderEntry1("Seq", "E"),
        BuilderEntry1("Set", "E"),
        BuilderEntry2ImplicitKey("SortedMap", "A", "B"),
        BuilderEntry1Implicit("SortedSet", "E"),
        BuilderEntry1("Traversable", "E")
      )),
    BuilderSet("HasImmutableBuilders", "scala.collection.immutable",
      List(
        SpecificBuilderEntry("BitSet", "Int"),
        BuilderEntry2("HashMap", "A", "B"),
        BuilderEntry1("HashSet", "E"),
        BuilderEntry1("IndexedSeq", "E"),
        BuilderEntry1("Iterable", "E"),
        BuilderEntry1("LinearSeq", "E"),
        BuilderEntry1("List", "E"),
        BuilderEntry2("ListMap", "A", "B"),
        BuilderEntry1("ListSet", "E"),
        BuilderEntry2("Map", "A", "B"),
        BuilderEntry1("Queue", "E"),
        BuilderEntry1("Seq", "E"),
        BuilderEntry1("Set", "E"),
        BuilderEntry2ImplicitKey("SortedMap", "A", "B"),
        BuilderEntry1Implicit("SortedSet", "E"),
        BuilderEntry1("Stack", "E"),
        BuilderEntry1("Stream", "E"),
        BuilderEntry1("Traversable", "E"),
        BuilderEntry2ImplicitKey("TreeMap", "A", "B"),
        BuilderEntry1("Vector", "E"))),
    BuilderSet("HasMutableBuilders", "scala.collection.mutable",
      List(
        BuilderEntry1("ArrayBuffer", "E"),
        BuilderEntry1("ArraySeq", "E"),
        SpecificBuilderEntry("BitSet", "Int"),
        BuilderEntry1("Buffer", "E"),
        BuilderEntry1("DoubleLinkedList", "E"),
        BuilderEntry2("HashMap", "A", "B"),
        BuilderEntry1("HashSet", "E"),
        BuilderEntry1("IndexedSeq", "E"),
        BuilderEntry1("Iterable", "E"),
        BuilderEntry1("LinearSeq", "E"),
        BuilderEntry2("LinkedHashMap", "A", "B"),
        BuilderEntry1("LinkedHashSet", "E"),
        BuilderEntry1("LinkedList", "E"),
        BuilderEntry1("ListBuffer", "E"),
        BuilderEntry2("ListMap", "A", "B"),
        BuilderEntry2("Map", "A", "B"),
        BuilderEntry1("ResizableArray", "E"),
        BuilderEntry1("Seq", "E"),
        BuilderEntry1("Set", "E"),
        BuilderEntry1("Traversable", "E"),
        BuilderEntry2("WeakHashMap", "A", "B")))
  )

  def main(args: Array[String]) {

    def fullCollName(builder: BuilderSet, entry: BuilderEntry) = 
      builder.prefix + "." + entry.coll
    def bracket(s: String): String = "[" + s + "]"

    println("package %s".format(`package`))
    for (builder <- builders) {
      println("trait %s {".format(builder.name))
      for (entry <- builder.entries) {
        val fullName = fullCollName(builder, entry)
        val escaped = fullName.replaceAll("\\.", "\\$")
        println("  implicit def %sFactory%s%s = new BuilderFactory[%s, %s%s] {".format(
              escaped, 
              entry.collTypeParam.map(bracket).getOrElse(""), 
              entry.implicitParams.map(x => "(implicit %s)".format(x)).getOrElse(""),
              entry.elemTypeParam,
              fullName, 
              entry.collTypeParam.map(bracket).getOrElse("")))
        println("    def newBuilder = %s.newBuilder%s".format(fullName, entry.builderTypeParam.map(bracket).getOrElse("")))
        println("  }")
      }
      println("}")
    }

  }
}
