count = 0
hostname = `hostname`.chomp

file = File.new("/mnt/vmstats.#{hostname}.csv", "w+")
stats = IO.popen('vmstat 1')
stats.readline

file.puts((["time", "host"] + stats.readline.split(" ")).join(","))



while(true)
  line = stats.readline
  if line =~ /\d/
    file.puts(([Time.now.to_i, hostname] + line.split(" ")).join(","))
    puts(([Time.now.to_i, hostname] + line.split(" ")).join(",")) if (count += 1) % 10 == 0
  end
end