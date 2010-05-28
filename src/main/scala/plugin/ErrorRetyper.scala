package com.googlecode.avro
package plugin

import scala.tools._
import nsc.Global
import nsc.Phase
import nsc.plugins.Plugin
import nsc.plugins.PluginComponent
import nsc.transform.Transform
import nsc.transform.InfoTransform
import nsc.transform.TypingTransformers
import nsc.symtab.Flags._
import nsc.util.Position
import nsc.ast.TreeDSL
import nsc.typechecker
import scala.annotation.tailrec

trait ErrorRetyper extends PluginComponent
  with Transform
  with TypingTransformers
{
  import global._
  import definitions._
  	  
  val runsAfter = List[String]("methodgen")
  override val runsBefore = List[String]("namer")
  val phaseName = "errorretyper"
  def newTransformer(unit: CompilationUnit) = new RetypingTransformer(unit)    

  object ErrorCleaner extends Traverser {
    override def traverse(tree: Tree) = {
      if (tree.tpe == ErrorType) tree.tpe = null
      super.traverse(tree)
    }
  } 
  
  class RetypingTransformer(unit: CompilationUnit) extends TypingTransformer(unit) {
    override def transform(tree: Tree) : Tree = {
      val fixedTree = if (tree.tpe == ErrorType) {
	 	ErrorCleaner.traverse(tree)
	 	localTyper.context1.reportGeneralErrors = true
	 	localTyper.typed { tree } 
	  } else { tree }
      
	  super.transform(fixedTree)
	}    
  }
}

