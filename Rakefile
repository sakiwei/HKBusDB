#!/usr/bin/ruby
# Rakefile

task default: [:build, :install]

task :clean do
  STDOUT.puts "Cleaning"
  DbPaths = ["bus.db", "bus_nwfb_ctb.db", "bus_kmb.db"]
  DbPaths.each do |path|
    File.delete(path) if File.exist?(path)
  end
  STDOUT.puts "Removed old database"
end

task :build => [:clean] do
  STDOUT.puts "Building"
  STDOUT.puts `ruby main_kmb.rb`
  STDOUT.puts `ruby main_nwfb_ctb.rb`
  STDOUT.puts `ruby main.rb`
end

task :install do
  STDOUT.puts "Installing"
end
