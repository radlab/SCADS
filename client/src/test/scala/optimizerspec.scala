package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.Compiler
import edu.berkeley.cs.scads.model.parser.ScalaCompiler
import edu.berkeley.cs.scads.model._


object QueryExecSpec extends SpecificationWithJUnit("PIQL Query Execution Specification"){
	"The SCADS Optimizer" should {
		implicit val env = new Environment
		env.placement = new TestCluster
		env.session = new TrivialSession
		env.executor = new TrivialExecutor

		"correctly execute queries that return" >> {
			"entities by primary key" in {
				implicit val loader = createLoader("ENTITY e1 {int a1 PRIMARY(a1)}\n QUERY q1 FETCH e1 WHERE a1 = [1:p]")
				val entities = (1 to 10).toList.map(i => {
					createEntity("e1", Map("a1" -> i))
				})

				(1 to 10).foreach(i => {
					List(entities(i)) must_== execQuery("q1",	i)
				})
			}

			"ascending sorted integers whith no join" in {
				implicit val loader = createLoader("ENTITY e1 {string a1, int a2 PRIMARY(a2)}\n QUERY q1 FETCH e1 ORDER BY a2 ASC LIMIT 10 MAX 10")
				val entities = (1 to 100).toList.map(i => {
					createEntity("e1", Map("a1" -> i.toString, "a2" -> i))
				})

				entities.slice(0, 10) must_== execQuery("q1")
			}
		}
	}

	def execQuery(name: String, args: Any*)(implicit loader: ClassLoader, env: Environment): Seq[Entity] = {
		val queryObject = loader.loadClass("Queries")
		val allArgs = args ++ Array(env)
		val argTypes = allArgs.map(_ match {
			case _: Int => classOf[Int]
			case r: AnyRef => r.getClass
		})
		val boxedArgs = allArgs.map(_ match {
			case i: Int => new java.lang.Integer(i)
			case r: AnyRef => r
		})
		val queryMethod = queryObject.getMethod("q1", argTypes: _*)

		queryMethod.invoke(queryObject, boxedArgs: _*).asInstanceOf[Seq[Entity]]
	}

	def createEntity(name: String, fields: Map[String, Any])(implicit loader: ClassLoader, env: Environment): Entity = {
		val e = loader.loadClass(name).getConstructors.apply(0).newInstance(env).asInstanceOf[Entity]
		fields.foreach(a => {
			a._2 match {
				case i: Int => e.attributes(a._1).asInstanceOf[IntegerField].apply(i)
				case s: String => e.attributes(a._1).asInstanceOf[StringField].apply(s)
			}
		})
		e.save
		return e
	}

	def createLoader(piqlSource: String): ClassLoader = {
		val scalaSource = Compiler.codeGenFromSource(piqlSource)
		val scalaCompiler = new ScalaCompiler
		scalaCompiler.compile(scalaSource)
		scalaCompiler.classLoader
	}
}

class QueryExecTest extends JUnit4(QueryExecSpec)
