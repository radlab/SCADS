package edu.berkeley.cs.scads.test

import org.scalatest.Suite
import edu.berkeley.cs.scads.model.reflected._

import edu.berkeley.cs.scads.model.Environment
import edu.berkeley.cs.scads.model.SingleGet
import edu.berkeley.cs.scads.model.ReadOneGetter
import edu.berkeley.cs.scads.model.Materialize
import edu.berkeley.cs.scads.model.LazyExecutor
import edu.berkeley.cs.scads.model.TrivialExecutor
import edu.berkeley.cs.scads.model.TrivialSession
import edu.berkeley.cs.scads.model.SequentialDereferenceIndex
import edu.berkeley.cs.scads.model.IntegerVersion
import edu.berkeley.cs.scads.model.Unversioned
import edu.berkeley.cs.scads.model.ReadOwnWrites
import edu.berkeley.cs.scads.model.SequentialDereferenceIndex
import edu.berkeley.cs.scads.placement.TrivialKeySpace
import edu.berkeley.cs.scads.nodes.TestableBdbStorageNode

object Profile {
	def find(username: String)(implicit env: Environment): Profile = {
		val query = new Materialize[Profile](
			new SingleGet("Profile", (new edu.berkeley.cs.scads.model.StringField)(username), new IntegerVersion) with ReadOneGetter)
		query.get.apply(0)
	}

	def findByName(name: String)(implicit env: Environment): Profile = {
		val query = new Materialize[Profile](
			new SequentialDereferenceIndex("Profile", new edu.berkeley.cs.scads.model.StringField, new IntegerVersion,
				new SingleGet("Profile$nameIndex$", (new edu.berkeley.cs.scads.model.StringField)(name), Unversioned) with ReadOneGetter with ReadOwnWrites)
			with ReadOneGetter)
		query.get.apply(0)
	}
}

class Profile(implicit env: Environment) extends Entity {
	object username extends StringField(this);username
	object name extends StringField(this);name

	val primaryKey = username

	object nameIndex extends FieldIndex(this, name);nameIndex
}

class EntityTest extends Suite {
	private def setupEnv: Environment = {
		implicit val env = new Environment
		env.placement = new TestCluster
		env.executor = new TrivialExecutor
		env.session = new TrivialSession
		return env
	}

	def testSerialization() = {
		implicit val env = setupEnv

		val michael = new Profile()
		michael.username("marmbrus")
		michael.name("michael")

		val michael2 = new Profile()
		michael2.deserializeAttributes(michael.serializeAttributes)
		michael.attributes.keys.foreach((k) => assert(michael.attributes(k) === michael2.attributes(k)))
	}

	def testSaveFind() = {
		implicit val env = setupEnv

		val michael = new Profile()
		michael.username("marmbrus")
		michael.name("michael")

		michael.save
		val michael2 = Profile.find("marmbrus")
		michael.attributes.keys.foreach((k) => assert(michael.attributes(k) === michael2.attributes(k)))
	}

	def testSimpleIndex() = {
		implicit val env = setupEnv

		val michael = new Profile()
		michael.username("marmbrus")
		michael.name("michael")

		michael.save
		val michael2 = Profile.findByName("michael")
		michael.attributes.keys.foreach((k) => assert(michael.attributes(k) === michael2.attributes(k)))

		michael.name("armbrust")
		michael.save
		val michael3 = Profile.findByName("armbrust")
		michael.attributes.keys.foreach((k) => assert(michael.attributes(k) === michael3.attributes(k)))

		intercept[Exception] {
			Profile.findByName("michael")
		}
		assert(true)
	}

	def testReadOwnWrites = {
		implicit val env = setupEnv
		env.executor = new LazyExecutor

		val beth = new Profile()
		beth.username("trush")
		beth.name("beth")

		beth.save
		val beth2 = Profile.findByName("beth")
		checkAttrs(beth, beth2)

		assert(true)
	}

	private def checkAttrs(e1: Entity, e2: Entity) {
		e1.attributes.keys.foreach((k) => assert(e1.attributes(k) === e2.attributes(k)))
	}
}
