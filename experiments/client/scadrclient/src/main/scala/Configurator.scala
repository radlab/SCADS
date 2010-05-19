package piql

import org.apache.avro.specific.SpecificRecordBase
import edu.berkeley.cs.scads.storage.{Namespace, TestScalaEngine}
import edu.berkeley.cs.scads.piql.Environment

class Configurator {
    def configureTestCluster(): Environment = {
        val env = new Environment
        env.namespaces = Map("ent_User" -> TestScalaEngine.cluster.getNamespace[User.KeyType, User.ValueType]("ent_User").asInstanceOf[Namespace[SpecificRecordBase,SpecificRecordBase]])
        env.namespaces += "ent_Thought" -> TestScalaEngine.cluster.getNamespace[Thought.KeyType, Thought.ValueType]("ent_Thought").asInstanceOf[Namespace[SpecificRecordBase,SpecificRecordBase]]
        env.namespaces += "ent_Subscription" -> TestScalaEngine.cluster.getNamespace[Subscription.KeyType, Subscription.ValueType]("ent_Subscription").asInstanceOf[Namespace[SpecificRecordBase,SpecificRecordBase]]
        env
    }
}
