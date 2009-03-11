
require 'thrift'
require 'thrift/protocol/binaryprotocol'
require 'thrift/server/tserver'
require 'timeout'

["simple", "record_set", "conflict_policy"].each do |file|
  require file
end

# get args 
host = ARGV[0]
port = ARGV[1]
if host.nil? or port.nil?
  puts "Missing arguments.\nUSAGE: start_server.rb host port"
  Process.exit
end

handler = SCADS::Storage::Simple::Handler.new()
puts "Setting up SCADS storage handler" if $DEBUG
processor = SCADS::Storage::Storage::Processor.new(handler)
puts "Opening socket on #{host}:#{port}" if $DEBUG
@transport = Thrift::ServerSocket.new(host,port)
transportFactory = Thrift::BufferedTransportFactory.new()
puts "Attempting to start server on #{host}:#{port}" if $DEBUG
@server = Thrift::SimpleServer.new(processor, @transport, transportFactory)

begin
  puts "Starting server on #{host}:#{port}"
  @server.serve
rescue Exception => e
  puts e
  puts e.backtrace
  raise "Server on #{host}:#{port} died"
end