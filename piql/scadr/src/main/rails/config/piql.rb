import Java::EduBerkeleyCsScadsPiqlScadr::ScadrClient
import Java::EduBerkeleyCsScadsPiql::SimpleExecutor
import Java::EduBerkeleyCsScadsStorage::TestScalaEngine

$PIQL_SCHEMA_PACKAGE = "EduBerkeleyCsScadsPiqlScadr"
$CLIENT = ScadrClient.new(TestScalaEngine.new_scads_cluster(1), SimpleExecutor.new, 10)
