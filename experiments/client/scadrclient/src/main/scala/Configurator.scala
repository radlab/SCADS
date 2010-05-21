package piql

import org.apache.avro.specific.SpecificRecordBase
import edu.berkeley.cs.scads.storage.{Namespace, TestScalaEngine}
import edu.berkeley.cs.scads.piql.Environment

class TestConfigurator {
    def configureTestCluster(): Environment = Configurator.configure(TestScalaEngine.cluster)
}
