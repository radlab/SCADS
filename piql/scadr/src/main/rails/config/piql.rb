import Java::EduBerkeleyCsScadsPiqlScadr::ScadrClient
import Java::EduBerkeleyCsScadsPiql::SimpleExecutor
import Java::EduBerkeleyCsScadsStorage::TestScalaEngine

$PIQL_SCHEMA_PACKAGE = "EduBerkeleyCsScadsPiqlScadr"
$CLIENT = ScadrClient.new($SCADS_CLUSTER, $PIQL_EXECUTOR, 10)
