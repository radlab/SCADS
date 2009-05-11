#!/usr/bin/ruby -rubygems

require 'sshctl'
REQUESTS = 500000/30
KEYRANGE = 50

class Array
  def map_with_index
    result = []
    self.each_with_index do |elt, idx|
      result << yield(elt, idx)
    end
    result
  end
end

[1,8,16,32].each do |scale|
  test_id = Time.now
  
  servers = InstanceGroup.new(request_nodes(scale))
  clients = InstanceGroup.new(request_nodes(scale))

  serverconf = default_config
  serverconf["recipes"] << "scads::dbs"
  serverconf["recipes"] << "scads::perf"
  serverconf["recipes"] << "scads::storage_engine"

  clientconf = default_config
  clientconf["recipes"] << "scads::perf"

  puts("deploying chef config")
  puts servers.deploy(serverconf)
  puts clients.deploy(clientconf)

  puts("copying jars")
  puts clients.copy_to("../../placement/dist/placement.jar", "/usr/share/java")

  dpservice = <<-EOF
val keyFormat = new java.text.DecimalFormat("000000000000000")
val dp = new SimpleDataPlacement("perfTest")
EOF

  dpservice += servers.instances.map_with_index {|s,i| "val n#{i} = new StorageNode(\"#{s.internal}\",9000,9091)"}.join("\n") + "\n"
  dpservice += servers.instances.map_with_index {|s,i| "dp.assign(n#{i}, KeyRange(keyFormat.format(#{(1024*KEYRANGE + 1)/scale*i}),keyFormat.format(#{(1024*KEYRANGE+1)/scale*(i+1)})))"}.join("\n") +"\n"

  dpservice += <<-EOF
val server = new KeySpaceServer(8000)
server.add("perfTest", dp)
println("KeyspaceServer up and running")
server.thread.join
EOF

  puts dpservice

  dpconf = default_config.merge({"recipes" => ["runit::default"], "services" => {"scads_data_placement" => dpservice}})
  puts servers[0].deploy(dpconf)
  puts servers[0].start("scads_data_placement")
  
  loadtest = "
val threads = (1 to 30).toList.map((id) => { new Thread(new RandomReader(\"#{scale} #{test_id}\", \"#{servers[0].internal}\", #{KEYRANGE}, #{REQUESTS}))})

for(thread <- threads) thread.start
for(thread <- threads) thread.join
println(\"done\")
"
  
  testerconf = default_config.merge({"recipes" => ["runit::default"], "services" => {"scads_loadtester" => loadtest}})
  
  clients.deploy(testerconf)
  
  sleep 10
  
  puts clients.stop("scads_loadtester")
  puts clients.once("scads_loadtester")
  
  sleep 30
  
  running = clients.instances
  
  while(running.size > 0)
    running = running.select {|n| n.services.select{|s| s[:name] == "scads_loadtester"}[0][:status] == "run"}
    puts "#{running.size} still running"
    sleep 10
  end
  
  servers.copy_from("/mnt/*.csv", ".")
  clients.copy_from("/mnt/*.csv", ".")
  
  servers.clean_services
  clients.clean_services
  
  servers.free
  clients.free
end