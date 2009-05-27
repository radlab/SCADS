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
          Timeout::timeout(30) do
            @client.send(symbol, *args)
          end
        end

        def stop
          Process.kill("KILL", @child)
          Process.waitpid(@child)
        end

        def host
          "localhost:#{@port}"
        end

        def sync_host
          "localhost:#{@sync_port}"
        end

        def start
          @port = rand(65000 - 1024) + 1024
          @sync_port = rand(65000 - 1024) + 1024
          @directory = "testdb/#{@port}"

          `rm -rf #{@directory}` if File.exists?(@directory)
          Dir.mkdir('testdb') unless File.exists?('testdb')
          Dir.mkdir(@directory)

          @child = Kernel.fork do
            $stdout.reopen(File.new("bdb.log", "a")) or
            puts "failed to redirect"
            $stderr.reopen(File.new("bdb.log", "a")) or
            puts "failed to redirect"
            exec "engines/bdb/storage.bdb -p #{@port} -l #{@sync_port} -d #{@directory}"
          end

          sleep 1

          transport = Thrift::FramedTransport.new(Thrift::Socket.new('localhost', @port))
          protocol = Thrift::BinaryProtocol.new(transport)
          transport.open
          @client = Storage::Client.new(protocol)
        end
      end
    end
  end
end
