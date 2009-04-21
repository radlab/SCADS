#As of 2009-03-28, this client is purely for speed comparisons - minimal error handling is performed
require 'getoptlong'
require 'gen-rb/Storage'

opts = GetoptLong.new(
  [ '--host', '-h', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--port', '-p', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--table', '-t', GetoptLong::REQUIRED_ARGUMENT ]
)

host = "localhost"
port = 9000
table = "default"
opts.each do |opt, arg|
  case opt
    when '--host'
      host = arg
    when '--port'
      port = arg.to_i
    when '--table'
      table = arg
  end
end

socket = Thrift::Socket.new(host, port);
transport = Thrift::BufferedTransport.new(socket);
protocol = Thrift::BinaryProtocol.new(transport)
transport.open
client = SCADS::Storage::Storage::Client.new(protocol)

$stdin.each_line do |line|
  line = line.split
  case line[0]
  when "table"
    before, after = 0,0
    table = result = (line[1] || table)
  when "get":
    before = Time.now
    result = client.get(table, line[1]).value
    after = Time.now
  when "put":
    r = SCADS::Storage::Record.new
    r.key = line[1]
    r.value = line[2]
    before = Time.now
    result = client.put(table, r)
    after = Time.now
  when "set_responsibility_policy":
    p = SCADS::Storage::RecordSet.new
    
    p.type = SCADS::Storage::RecordSetType::RST_KEY_FUNC
    uf = SCADS::Storage::UserFunction.new
    uf.lang = SCADS::Storage::Language::LANG_RUBY
    uf.func = line[1..-1].join(" ")
    puts uf.func
    p.func = uf
    
    before = Time.now
    result = client.set_responsibility_policy(table, p)
    after = Time.now
  when "get_responsibility_policy":
    before = Time.now
    result = client.get_responsibility_policy(table)
    puts result.func.inspect
    after = Time.now
  else
    raise "Invalid command: #{line[0]}"
  end
  puts (after - before).to_s + "\t" + line.join("\t") + "\t=>\t" + result.to_s
end

transport.close#As of 2009-03-28, this client is purely for speed comparisons - minimal error handling is performed