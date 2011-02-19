import Java::EduBerkeleyCsScadsPiqlComrades::ComradesClient
import Java::EduBerkeleyCsScadsPiql::SimpleExecutor
import Java::EduBerkeleyCsScadsStorage::TestScalaEngine

$PIQL_SCHEMA_PACKAGE = "EduBerkeleyCsScadsPiqlComrades"
$CLIENT = ComradesClient.new($SCADS_CLUSTER, $PIQL_EXECUTOR)