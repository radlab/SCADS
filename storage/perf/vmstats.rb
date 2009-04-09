stats = IO.popen('vmstat 1')
stats.readline

puts (["time", "host"] + stats.readline.split(" ")).join(",")

hostname = `hostname`.chomp

while(true)
  line = stats.readline
  puts ([Time.now.to_i, hostname] + line.split(" ")).join(",") if line =~ /\d/
end