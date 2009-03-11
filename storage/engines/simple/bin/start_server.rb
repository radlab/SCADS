#!/usr/bin/env ruby

require 'thrift'
require 'thrift/protocol/binaryprotocol'
require 'thrift/server/tserver'
require 'timeout'
require 'optparse'
# 
# ["simple", "record_set", "conflict_policy"].each do |file|
#   require file
# end
require 'scads'

# get commnd line args
opts = {:host=>'localhost'}
ARGV.options do |o|
  o.set_summary_indent('  ')
  o.banner =    "Usage: #{File.basename($0)} [opts]"
  o.separator   ""
  o.on("-s", "--host=name", String,
       "Server host name (optional)",
       "Default: #{opts[:host]}") do |t| 
          opts[:host] = t
        end 
  o.separator ""
  o.on("-p", "--port=num", Integer,
       "Port number (required)")  do |p| 
          opts[:port] = p
        end
  o.separator ""
  o.on_tail("-h", "--help", "Show this help message") { puts o; Process.exit }
  o.parse!
end

# check have all args 
host = opts[:host]
port = opts[:port]
if host.nil? or port.nil?
  puts "Specify arguments. See #{File.basename($0)} -h for help."
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