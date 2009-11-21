package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.Compiler
import edu.berkeley.cs.scads.model.parser.ScalaCompiler
import edu.berkeley.cs.scads.model._

import org.apache.log4j.Logger

object QueryExecSpec extends SpecificationWithJUnit("PIQL Query Execution Specification"){
	val log = Logger.getLogger("scads.test.queryexec")
	val rand = new scala.util.Random

	"The SCADS Optimizer" should {
		implicit val env = new Environment
		env.placement = new TestCluster
		env.session = new TrivialSession
		env.executor = new TrivialExecutor

		"correctly execute queries that return" >> {
			"entities by primary key" in {
				implicit val loader = createLoader("ENTITY e1 {int a1 PRIMARY(a1)}\n QUERY q1 FETCH e1 WHERE a1 = [1:p]")
				val entities = (0 to 10).toList.map(i => {
					createEntity("e1", Map("a1" -> i))
				})

				(0 to 10).foreach(i => {
					List(entities(i)) must_== execQuery("q1",	i)
				})
			}

			"sorted integers with no join" >> {
				implicit val loader = createLoader("""
					ENTITY e1 {int a1, int a2 PRIMARY(a1)}
					QUERY q1 FETCH e1 ORDER BY a2 ASC LIMIT 10 MAX 10
					QUERY q2 FETCH e1 ORDER BY a2 DESC LIMIT 10 MAX 10""")

				val entities = (1 to 100).toList.map(i => {
					createEntity("e1", Map("a1" -> rand.nextInt, "a2" -> i))
				})

				"ascending" in {
					execQuery("q1") must containInOrder(entities.slice(0,10))
				}

				"decending" in {
					execQuery("q2") must containInOrder(entities.reverse.slice(0,10))
				}
			}

			"sorted integers with prefix join" >> {
				implicit val loader = createLoader("""
					ENTITY e1 {int a1 PRIMARY(a1)}
					ENTITY e2 {int a1, int a2 PRIMARY(r,a1)}
					RELATIONSHIP r FROM e1 TO MANY e2
					QUERY q1 FETCH e2 OF e1 BY r WHERE e1 = [this] ORDER BY a2 ASC LIMIT 10 MAX 10
					QUERY q2 FETCH e2 OF e1 BY r WHERE e1 = [this] ORDER BY a2 DESC LIMIT 10 MAX 10""")

				val e1 = createEntity("e1", Map("a1" -> 1))

				val entities = (1 to 100).toList.map(i => {
					createEntity("e2", Map("a1" -> rand.nextInt, "a2" -> i, "r" -> e1))
				})

				"ascending" in {
					execQuery(e1, "q1") must containInOrder(entities.slice(0,10))
				}

				"descending" in {
					execQuery(e1, "q2") must containInOrder(entities.reverse.slice(0,10))
				}

			}
		}
	}

	def execQuery(name: String, args: Any*)(implicit loader: ClassLoader, env: Environment): Seq[Entity] = {
		val queryObject = loader.loadClass("Queries")
		execQuery(queryObject, name, args:_*)
	}

	def execQuery(obj: AnyRef, name: String, args: Any*)(implicit loader: ClassLoader, env: Environment): Seq[Entity] = {
		val allArgs = args ++ Array(env)
		val argTypes = allArgs.map(_ match {
			case _: Int => classOf[Int]
			case r: AnyRef => r.getClass
		})
		val boxedArgs = allArgs.map(_ match {
			case i: Int => new java.lang.Integer(i)
			case r: AnyRef => r
		})
		val queryMethod = obj match {
			case c: Class[_] => c.getMethod("q1", argTypes: _*)
			case a: AnyRef => a.getClass.getMethod("q1", argTypes: _*)
		}

		queryMethod.invoke(obj, boxedArgs: _*).asInstanceOf[Seq[Entity]]
	}

	def createEntity(name: String, fields: Map[String, Any])(implicit loader: ClassLoader, env: Environment): Entity = {
		val e = loader.loadClass(name).getConstructors.apply(0).newInstance(env).asInstanceOf[Entity]
		fields.foreach(a => {
			a._2 match {
				case i: Int => e.attributes(a._1).asInstanceOf[IntegerField].apply(i)
				case s: String => e.attributes(a._1).asInstanceOf[StringField].apply(s)
				case ent: Entity => e.attributes(a._1).apply(ent)
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
