count = 0
hostname = `hostname`.chomp

file = File.new("/mnt/vmstats.#{hostname}.csv", "w+")
stats = IO.popen('vmstat 1')
stats.readline

file.puts((["time", "host", "tx", "rx"] + stats.readline.split(" ")).join(","))



while(true)
  line = stats.readline
  ifconfig = `ifconfig eth0`

  if line =~ /\d/
    file.puts(([Time.now.to_i, hostname, ifconfig[/RX bytes:(\d+)/,1], ifconfig[/TX bytes:(\d+)/,1]] + line.split(" ")).join(","))
    puts(([Time.now.to_i, hostname, ifconfig[/RX bytes:(\d+)/,1], ifconfig[/TX bytes:(\d+)/,1]] + line.split(" ")).join(",")) if (count += 1) % 10 == 0
  end
  file.flush
  STDOUT.flush
end
