require 'thrift'
require 'thrift/protocol/binaryprotocol'
require 'thrift/server/tserver'

require 'timeout'

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
          Timeout::timeout(5) do
            @client.send(symbol, *args)
          end
        end

        def stop
          @thread.kill
          @transport.close
        end

        def start
          @thread = Thread.new do 
            while true
              @port = rand(65000 - 1024) + 1024

              handler = Handler.new()
              processor = Storage::Processor.new(handler)
              @transport = Thrift::ServerSocket.new(@port)
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

          transport = Thrift::BufferedTransport.new(Thrift::Socket.new('localhost', @port))
          protocol = Thrift::BinaryProtocol.new(transport)
          transport.open
          @client = Storage::Client.new(protocol)
        end
      end
    end
  end
end