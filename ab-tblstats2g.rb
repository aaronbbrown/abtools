#!/usr/bin/env ruby

require 'pp'
require 'lib/string'
require 'lib/defaults'
require 'logger'
require 'optparse'

require 'rubygems'
require 'graphite/logger' # gem install graphite
require 'sequel'          # gem install sequel

module ABTableStats
  module Helper
    # convert a String value size to a numeric bytes value
    # 
    # Examples:
    #   to_bytes("10 KB")
    #   # => 10240
    #
    def to_bytes(str)
      begin
        a = str.split    

        multiplier = case a[1] 
        when "B"  then 1
        when "KB" then 2**10
        when "MB" then 2**20
        when "GB" then 2**30
        when "TB" then 2**40
        else 1
        end

        return a[0].to_i * multiplier
      rescue
        return 0        
      end
    end
  end

  class Base
    include Helper
    attr_accessor :conn

    def initialize ( h )
      sequel_opts = {
        :database => h[:database],
        :adapter  => h[:adapter],
        :host     => h[:dsn]['h']      || 'localhost',
        :socket   => h[:dsn]['S'],
        :user     => h[:dsn]['u'], 
        :password => h[:dsn]['p'],
        :port     => h[:dsn]['P'].to_i,
        :logger   => h[:logger]
      }
      @conn = Sequel.connect(sequel_opts)
    end
  end

  class MySQL < Base
    def initialize (h)
      h.merge!(
        :database => "INFORMATION_SCHEMA",
        :adapter  => 'mysql2'
      )
      @prefix = h[:prefix] || 'mysql'
      super h
    end

    def get_table_stats
      stats = {}
      ignore_schemas = %w[INFORMATION_SCHEMA mysql PERFORMANCE_SCHEMA]
      query = <<SQL
SELECT TABLE_SCHEMA, TABLE_NAME, DATA_LENGTH, 
       INDEX_LENGTH, (DATA_LENGTH+INDEX_LENGTH) AS TOTAL_LENGTH
       FROM INFORMATION_SCHEMA.TABLES
       WHERE TABLE_SCHEMA NOT IN ?
SQL

      @conn[query,ignore_schemas].each do |row|
        key_prefix = [ @prefix,
                       row[:TABLE_SCHEMA],
                       row[:TABLE_NAME] ].compact.join('.')

        stats["#{key_prefix}.data_bytes"]  = row[:DATA_LENGTH]  || 0
        stats["#{key_prefix}.index_bytes"] = row[:INDEX_LENGTH] || 0
        stats["#{key_prefix}.total_bytes"] = row[:TOTAL_LENGTH] || 0
      end
      stats
    end
  end

  class MSSQL < Base
    def initialize ( h )
      require 'tiny_tds'
      h.merge!( :database => 'master',
                :adapter  => 'tinytds' )
      @prefix = h[:prefix] || 'mssql'
      super h
    end

    def get_database_stats
      @conn['SELECT DB_NAME(database_id) as db_name, (size*8) as size_kb FROM sys.master_files']
    end

    def get_databases
      ignore_schemas = %w[tempdb master]
      @conn['SELECT name FROM sys.databases WHERE name NOT IN ?', ignore_schemas].map { |x| x[:name] }
    end

    def get_table_stats_for_db ( db )
      stats = {}
        @conn.execute("USE [#{db}]")
        space_used = @conn["EXEC sp_MSforeachtable 'EXEC sp_spaceused ''?'''"].all
        space_used.each do |row|
          key_prefix = [ @prefix, db,
                         row[:name] ].compact.join('.')

          stats["#{key_prefix}.data_bytes"]   = to_bytes(row[:data])
          stats["#{key_prefix}.index_bytes"]  = to_bytes(row[:index_size])
          stats["#{key_prefix}.unused_bytes"] = to_bytes(row[:unused])
          stats["#{key_prefix}.total_bytes"]  = to_bytes(row[:data]) + to_bytes(row[:index_size])
          stats["#{key_prefix}.rows"]         = row[:rows].to_i || 0
        end
        stats
    end

    def get_table_stats
      stats = {}
      dbs = get_databases
      dbs[0..2].each do |db|
        stats.merge!(get_table_stats_for_db(db))
      end
      stats
    end
  end
end

options = { :server_type => 'mysql' }

opts = OptionParser.new
opts.banner = "Usage: #{$0} [OPTIONS]"
opts.on("-s", "--server-type TYPE", String, 
    "Server type.  Can be 'mysql' or 'mssql'.  (default: #{options[:server_type]})") do |v|
  options[:server_type] = v
end
opts.on("-d", "--dsn DSN", String, "Connection DSN") do |v|
  options[:dsn] = v.to_h
end
opts.on("-g", "--graphite HOST", String, "Graphite (carbon) host:port") do |v|
  options[:graphite] = v
end
opts.on("-p", "--prefix PREFIX", String, "Key prefix (defaults to server-type)" ) do |v|
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

options[:logger] = logger
if options[:server_type] == 'mssql'
  abts = ABTableStats::MSSQL.new(options)
else 
  abts = ABTableStats::MySQL.new(options)
end
conn = abts.conn

graphite = Graphite::Logger.new(options[:graphite])
graphite.logger = logger
stats = abts.get_table_stats
pp stats
graphite.log(Time.now, stats)
