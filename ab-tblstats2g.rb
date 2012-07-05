#!/usr/bin/env ruby

require 'pp'
require 'lib/string'
require 'lib/defaults'
require 'lib/tblstats'
require 'logger'
require 'optparse'

require 'rubygems'
require 'graphite/logger' # gem install graphite
require 'sequel'          # gem install sequel

def parse_dsn ( dsn )
  dsn_h = dsn.to_h
  { :user     => dsn_h['u'],
    :password => dsn_h['p'],
    :host     => dsn_h['h'],
    :socket   => dsn_h['s'],
    :port     => dsn_h['P'],
  }
end

options = { :server_type => 'mysql' }

opts = OptionParser.new
opts.banner = "Usage: #{$0} [OPTIONS]"
opts.on("-s", "--server-type TYPE", String, 
    "Server type.  Can be 'mysql' or 'mssql'.  (default: #{options[:server_type]})") do |v|
  options[:server_type] = v
end
opts.on("-d", "--dsn DSN", String, "Connection DSN") do |v|
  options.merge! parse_dsn(v)
end
opts.on("-g", "--graphite HOST", String, "Graphite (carbon) host:port") do |v|
  options[:graphite] = v
end
opts.on("-p", "--prefix PREFIX", String, "Key prefix (defaults to server-type)" ) do |v|
  options[:prefix] = v
end
opts.on("--defaults-file FILE", String, "MySQL style defaults file (ini format)") do |v|
  options.merge! ABTools.read_my_cnf(v)
end
opts.on("-h", "--help", "This message") { puts opts; exit 1 }
opts.parse!

unless options[:graphite]
  STDERR.puts "You must specify --graphite"
  puts opts
  exit 1
end

logger = Logger.new(STDOUT)
logger.level = Logger::DEBUG

options[:logger] = logger
if options[:server_type] == 'mssql'
  abts = ABTableStats::MSSQL.new(options)
else 
  abts = ABTableStats::MySQL.new(options)
end

graphite = Graphite::Logger.new(options[:graphite])
graphite.logger = logger
graphite.log(Time.now, abts.stats)
