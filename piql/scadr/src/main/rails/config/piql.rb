import Java::EduBerkeleyCsScadsPiqlScadr::ScadrClient
import Java::EduBerkeleyCsScadsPiql::SimpleExecutor
import Java::EduBerkeleyCsScadsStorage::TestScalaEngine
import Java::EduBerkeleyCsRadlabDemo::DemoConfig

$PIQL_SCHEMA_PACKAGE = "EduBerkeleyCsScadsPiqlScadr"
$CLIENT = ScadrClient.new($SCADS_CLUSTER, $PIQL_EXECUTOR, 10)

if(File::exists?($ALLJARS))
  puts "local cluster detected loading sample data"
  DemoConfig.loadScadrData($CLIENT)
end

puts "Loading example users"
$EXAMPLE_USERS = []
YAML.load("
-
  username:         marmbrus
  home_town:        Berkeley
  plain_password:   test
  confirm_password: test
-
  username:         rniwa
  home_town:        Berkeley
  plain_password:   test
  confirm_password: test
-
  username:         karl
  home_town:        Berkeley
  plain_password:   test
  confirm_password: test
").each do |user_data|
  if User.find(user_data['username']).nil?
    user = User.new(user_data)
    user.save

    thought = Thought.new
    thought.owner = user.username
    thought.timestamp = Time.now.to_i
    thought.text = "Hello, world!"
    thought.save

    $EXAMPLE_USERS.push user
  end
end
