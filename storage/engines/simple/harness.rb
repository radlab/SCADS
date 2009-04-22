require 'thrift'
require 'thrift/protocol/binaryprotocol'
require 'thrift/server/nonblockingserver'

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
          transport = Thrift::FramedTransport.new(Thrift::Socket.new('localhost', @port))
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
              handler = SCADS::Storage::Simple::Handler.new()
              puts "Setting up SCADS storage handler" if $DEBUG
              processor = SCADS::Storage::Storage::Processor.new(handler)
              puts "Opening socket on #{host}:#{port}" if $DEBUG
              @transport = Thrift::ServerSocket.new("0.0.0.0",@port)
              transportFactory = Thrift::FramedTransportFactory.new()
              puts "Attempting to start server on #{host}:#{port}" if $DEBUG
              @server = Thrift::NonblockingServer.new(processor, @transport, transportFactory)

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
      end
    end
  end
end