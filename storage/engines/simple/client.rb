# $:.push('../../gen-rb')
# $:.push('.')
require 'thrift'
require 'thrift/protocol/binaryprotocol'
require 'thrift/server/tserver'

require 'timeout'
require 'Storage'
["simple", "record_set", "conflict_policy"].each do |file|
   file
end

module SCADS
  class Client
    
    def initialize(host,port)
      @port = port
      @host = host
    end
    
    def method_missing(symbol, *args)
      puts "Attempting connection to #{@host}:#{@port}" if $DEBUG
      transport = Thrift::BufferedTransport.new(Thrift::Socket.new(@host, @port))
      protocol = Thrift::BinaryProtocol.new(transport)
      transport.open
      client = SCADS::Storage::Storage::Client.new(protocol)

      puts "Calling #{symbol} with #{args.inspect}" if $DEBUG
      ret = nil

      begin
        Timeout::timeout(5) do
          ret = client.send(symbol, *args)
        end
      rescue Exception =>e
        transport.close
        raise e
      end

      transport.close
      return ret
    end
  end
end

# module SCADS
#   module Storage
#     module Simple
#       class Node
#         def initialize(host,port)
#           @port = port
#           @host = host
#           start
#         end
# 
#         def method_missing(symbol, *args)
#           puts "Attempting connection to #{@port}" if $DEBUG
#           transport = Thrift::BufferedTransport.new(Thrift::Socket.new(@host, @port))
#           protocol = Thrift::BinaryProtocol.new(transport)
#           transport.open
#           client = Storage::Client.new(protocol)
# 
#           puts "Calling #{symbol} with #{args.inspect}" if $DEBUG
#           ret = nil
# 
#           begin
#             Timeout::timeout(5) do
#               ret = client.send(symbol, *args)
#             end
#           rescue Exception =>e
#             transport.close
#             raise e
#           end
# 
#           transport.close
#           return ret
#         end
# 
#         def stop
#           @transport.close
#           @thread.kill
#         end
#         
# 
#         def start
#           @thread = Thread.new do 
#             while true
#               handler = Handler.new()
#               processor = Storage::Processor.new(handler)
#               @transport = Thrift::ServerSocket.new(@port)
#               transportFactory = Thrift::BufferedTransportFactory.new()
#               @server = Thrift::SimpleServer.new(processor, @transport, transportFactory)
# 
#               begin
#                 @server.serve
#               rescue Exception => e
#                 puts e
#                 puts e.backtrace
#                 raise "server died"
#               end  
#             end    
#           end
#         end
# 
#         def host
#           return "#{@host}:#{@port}"
#         end
#       end
#     end
#   end
# end