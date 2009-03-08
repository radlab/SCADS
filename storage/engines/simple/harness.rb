require 'thrift'
require 'thrift/protocol/binaryprotocol'
require 'thrift/server/tserver'

["simple", "record_set", "conflict_policy"].each do |file|
  require File.dirname(__FILE__) + "/" + file
end

module SCADS
  module Storage
    module Simple
      class TestHarness
        def initialize
          start
        end

        def method_missing(symbol, *args)
          @client.send(symbol, *args)
        end

        def stop
          @thread.kill
          @transport.close
        end

        def start
          @thread = Thread.new do 
            while true
              handler = Handler.new()
              processor = Storage::Processor.new(handler)
              @transport = Thrift::ServerSocket.new(9090)
              transportFactory = Thrift::BufferedTransportFactory.new()
              @server = Thrift::SimpleServer.new(processor, @transport, transportFactory)
              begin
                @server.serve
              rescue Exception => e
                puts e
                puts e.backtrace
                raise "server died"
              end  
            end    
          end

          transport = Thrift::BufferedTransport.new(Thrift::Socket.new('localhost', 9090))
          protocol = Thrift::BinaryProtocol.new(transport)
          transport.open
          @client = Storage::Client.new(protocol)
        end
      end
    end
  end
end