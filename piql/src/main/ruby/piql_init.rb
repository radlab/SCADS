allJarsFile = File.join(RAILS_ROOT, "../../../../../allJars")

if(File::exists?(allJarsFile))
  puts "Loading local PIQL Jars"
  jars = File.read(allJarsFile).split("\n")
  jars.each {|j| puts j ; require j}
else
  puts "allJars file not found.  using provided piql libraries"
end
 
require File.join(RAILS_ROOT, "lib/avro_record")
require File.join(RAILS_ROOT, "config/piql")
