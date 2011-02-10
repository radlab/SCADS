allJarsFile = File.join(RAILS_ROOT, "../../../../../allJars")

if(File::exists?(allJarsFile))
  puts "Loading local PIQL Jars"
  jars = File.read(allJarsFile).split("\n")
  jars.each {|j| require j}
  puts "PIQL Jars Loaded"
else
  puts "allJars file not found.  using provided piql libraries"
end


import Java::EduBerkeleyCsScadsPiql::SimpleExecutor
import Java::EduBerkeleyCsScadsStorage::TestScalaEngine
import Java::EduBerkeleyCsScadsStorage::ScadsCluster
import Java::EduBerkeleyCsScadsComm::ZooKeeperNode
import Java::EduBerkeleyCsRadlabDemo::DashboardReportingExecutor

puts "Setting up executor and scads cluster client"
if(java.lang.System.getProperty("scads.clusterAddress").nil?)
  puts "Using test scala engine"
  $PIQL_EXECUTOR = SimpleExecutor.new
  $SCADS_CLUSTER = TestScalaEngine.newScadsCluster(1)
else
  cluster_address = java.lang.System.getProperty("scads.clusterAddress")
  puts "using scads cluster at: " + cluster_address 
  $PIQL_EXECUTOR = DashboardReportingExecutor.new
  $CLUSTER_ROOT = ZooKeeperNode.apply(cluster_address)
  $SCADS_CLUSTER = ScadsCluster.new($CLUSTER_ROOT)
end

puts "Loading avro_record library"
require File.join(RAILS_ROOT, "lib/avro_record")

puts "Loading local configuration"
require File.join(RAILS_ROOT, "config/piql")
