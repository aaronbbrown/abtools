#!/usr/bin/env ruby

require 'pp'
require 'lib/string'
require 'lib/defaults'
require 'logger'
require 'optparse'

require 'rubygems'
require 'graphite/logger' # gem install graphite
require 'sequel'          # gem install sequel

options = { :prefix => 'mysql' }

opts = OptionParser.new
opts.banner = "Usage: #{$0} [OPTIONS]"
opts.on("-d", "--dsn DSN", String, "Connection DSN") do |v|
  options[:dsn] = v.to_h
end
opts.on("-g", "--graphite HOST", String, "Graphite (carbon) host:port") do |v|
  options[:graphite] = v
end
opts.on("-p", "--prefix PREFIX", String, "Key prefix (default #{options[:prefix]})") do |v|
  options[:prefix] = v
end
opts.on("-h", "--help", "This message") { puts opts; exit 1 }
opts.parse!

unless options[:dsn] && options[:graphite]
  STDERR.puts "You must specify a --dsn and --graphite"
  puts opts
  exit 1
end

logger = Logger.new(STDOUT)
logger.level = Logger::DEBUG


sequel_opts = {
  :database => "INFORMATION_SCHEMA",
  :adapter  => 'mysql2',
  :host     => options[:dsn]['h'] || 'localhost',
  :port     => options[:dsn]['P'].to_i || 3306,
  :socket   => options[:dsn]['S'],
  :user     => options[:dsn]['u'], 
  :password => options[:dsn]['p'],
  :logger   => logger,
}

stats = {}
conn = Sequel.connect(sequel_opts)
graphite = Graphite::Logger.new(options[:graphite])
graphite.logger = logger

ignore_schemas = %w[INFORMATION_SCHEMA mysql PERFORMANCE_SCHEMA]
query = <<SQL
SELECT TABLE_SCHEMA, TABLE_NAME, DATA_LENGTH, 
       INDEX_LENGTH, (DATA_LENGTH+INDEX_LENGTH) AS TOTAL_LENGTH
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA NOT IN ?
SQL

conn[query,ignore_schemas].each do |row|
  key_prefix = [ options[:prefix],
                 row[:TABLE_SCHEMA],
                 row[:TABLE_NAME] ].compact.join('.')

  stats["#{key_prefix}.data_bytes"]  = row[:DATA_LENGTH]  || 0
  stats["#{key_prefix}.index_bytes"] = row[:INDEX_LENGTH] || 0
  stats["#{key_prefix}.total_bytes"] = row[:TOTAL_LENGTH] || 0
end

graphite.log(Time.now, stats)
