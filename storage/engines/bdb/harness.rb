require 'thrift'
require 'thrift/protocol/binaryprotocol'
require 'thrift/server/tserver'

require 'timeout'

module SCADS
  module Storage
    module Bdb
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
          Process.kill("TERM", @child)
          Process.waitpid(@child)
        end

        def start
          port = rand(65000 - 1024) + 1024
          
          @child = Kernel.fork do
            exec "engines/bdb/storage.bdb -p #{port}"
          end

          sleep 1

          transport = Thrift::BufferedTransport.new(Thrift::Socket.new('localhost', port))
          protocol = Thrift::BinaryProtocol.new(transport)
          transport.open
          @client = Storage::Client.new(protocol)
        end
      end
    end
  end
end