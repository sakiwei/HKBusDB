#!/usr/bin/ruby
# Rakefile

task default: [:build, :install]

task :clean do
  STDOUT.puts "Cleaning"
  DbPaths = ["bus.db", "bus_ctb.db", "bus_kmb.db"]
  DbPaths.each do |path|
    File.delete(path) if File.exist?(path)
  end
  STDOUT.puts "Removed old database"
end

task :build => [:clean] do
  STDOUT.puts "Building"
  STDOUT.puts `bundle exec ruby main_kmb.rb`
  STDOUT.puts `bundle exec ruby main_ctb.rb`
  STDOUT.puts `bundle exec ruby main.rb`
end

task :install do
  STDOUT.puts "Installing"
end
