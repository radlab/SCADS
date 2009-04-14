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
          puts "Attempting connection to #{@port}" if $DEBUG
          transport = Thrift::BufferedTransport.new(Thrift::Socket.new('localhost', @port))
          protocol = Thrift::BinaryProtocol.new(transport)
          transport.open
          client = Storage::Client.new(protocol)

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

        def stop
          @thread.kill
        end

        def start
          @port = rand(65000 - 1024) + 1024

          @thread = Thread.new do 
            while true
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
        end

        def host
          return "localhost:#{@port}"
        end

        def sync_host
          return "localhost:#{@port}"
        end
      end
    end
  end
end