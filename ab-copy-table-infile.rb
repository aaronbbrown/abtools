#!/usr/bin/env ruby
# Script to copy a table from one server to another using a different name
# Source an destination DSNs can be different, but must share disk
# for the exported file

require 'pp'
require 'rubygems'
require 'sequel'
require 'optparse'
require 'logger'

class String
# Public: return a Hash from a delimited String of k/v pairs.
#
# str            - a pair_delimiter delimited String of key value pairs
# pair_delimiter - (default: ,)
# kv_delimiter   - (default: =)
#
# Examples
#
#   "u=user,p=pass,s=socket".to_h
#   # => {"p"=>"pass", "s"=>"socket", "u"=>"user"} 
#
# Returns a Hash from the values passed in
  def to_h (pair_delimiter = ',', kv_delimiter = '=')
    Hash[self.split(pair_delimiter).map { |x| x.split(kv_delimiter) }]
  end
end

# Public: Build a filename from a Sequel connection, timestamp, and table name String
#
# db  - The Sequel object to use to build the connection
# tbl - A String of the table name
#
# Returns a String that is the basename of a file
def outfile_fn ( db, tbl )
  connection_id = db["SELECT CONNECTION_ID() AS connection_id"].first[:connection_id]
  timestamp = Time.new.strftime('%Y%m%d%H%M%S')
  "#{connection_id}_#{timestamp}_#{tbl}.txt"
end

# Public: Given a table name, escape it properly for MySQL
#
# tbl - the String name of the table
#
# Examples
#
#   escape_table_name("db.tbl")
#   # => "`db`.`tbl`"
#
#   escape_table_name("tbl")
#   # => "`tbl`"
#
# Returns a String that is useable in a MySQL SQL Statement
#
def escape_table_name (tbl)
  tbl.split('.').map { |x| '`'+x+'`' }.join('.')
end

def rewrite_create_table ( db, source_tbl, dest_tbl )
  ds = db["SHOW CREATE TABLE #{source_tbl}"]
  create_sql = ds.first[:'Create Table']
  create_sql.sub("CREATE TABLE `#{ds.first[:Table]}`", "CREATE TABLE #{dest_tbl}")
end

options = { :source_dsn         => nil,
            :source_tables      => nil,
            :destination_dsn    => nil,
            :destination_tables => nil,
            :outfile_dir        => '/tmp',
          }

opts = OptionParser.new
opts.banner = "Usage: #{$0} [OPTIONS]"
opts.on("-s", "--source-dsn DSN", String, "Source DSN") do |v|
  options[:source_dsn] = v.to_h
end
opts.on("-d", "--destination-dsn DSN", String, "Destination DSN") do |v|
  options[:destination_dsn] = v.to_h
end
opts.on("-t", "--source-tables TABLES", Array, "Comma separated list of source table names") do |v|
  options[:source_tables] = v
end
opts.on("-T", "--destination-tables TABLES", Array, "Comma separated list of destination table names") do |v|
  options[:destination_tables] = v
end
opts.on("-o", "--outfile-dir DIRECTORY", Array, "Destination directory for temporary files") do |v|
  options[:outfile_dir] = v
end
opts.on("-h", "--help", "This message") { puts opts; exit 1 }
opts.parse!

# validate some assumptions
options[:destination_dsn]    ||= options[:source_dsn]
options[:destination_tables] ||= options[:source_tables]

unless options[:source_dsn] &&
       options[:destination_dsn] &&
       options[:source_tables] &&
       options[:destination_tables]
  puts opts
  exit 1
end

unless options[:destination_tables].size == options[:destination_tables].size
  STDERR.puts "Number of source and destination tables must match!"
  STDERR.puts "You specified #{options[:source_tables].size} source & #{options[:destination_tables].size} destination tables"
  puts opts
  exit 1
end

logger = Logger.new(STDOUT)
logger.level = Logger::DEBUG

source = Sequel.mysql( options[:source_dsn]['d'], { 
    :host     => options[:source_dsn]['h'] || 'localhost',
    :user     => options[:source_dsn]['u'],
    :password => options[:source_dsn]['p'],
    :port     => options[:source_dsn]['P'] || 3306,
    :socket   => options[:source_dsn]['s']
    } )
source.loggers << logger

destination = Sequel.mysql( options[:destination_dsn]['d'], { 
    :host     => options[:destination_dsn]['h'] || 'localhost',
    :user     => options[:destination_dsn]['u'],
    :password => options[:destination_dsn]['p'],
    :port     => options[:destination_dsn]['P'] || 3306,
    :socket   => options[:destination_dsn]['s']
    } )
destination.loggers << logger

options[:source_tables].each_with_index do |tbl,i| 
  source_tbl = escape_table_name(tbl)
  dest_tbl   = escape_table_name(options[:destination_tables][i])
  backup_tbl = escape_table_name("#{options[:destination_tables][i]}_backup")
  outfile    = options[:outfile_dir] + "/" + outfile_fn(source, tbl)

  # get table definition
  create_sql = rewrite_create_table(source, source_tbl, dest_tbl)
  begin
    trap("INT") do 
      raise
    end
    logger.info "Dumping #{source_tbl}..."
    source["SELECT * FROM #{source_tbl} INTO OUTFILE '#{outfile}'"].all
    logger.info "Dropping old backup table #{backup_tbl}..."
    destination["DROP TABLE IF EXISTS #{backup_tbl}"].delete
    logger.info "Creating new table #{dest_tbl}..."
    destination["CREATE TABLE IF NOT EXISTS #{dest_tbl} (dummy int)"].insert
    destination["ALTER TABLE #{dest_tbl} RENAME TO #{backup_tbl}"].all
    logger.info "Creating new table #{dest_tbl}..."
    destination[create_sql].all
    logger.info "Loading data into #{dest_tbl}..."
    destination["LOAD DATA INFILE '#{outfile}' INTO TABLE #{dest_tbl}"].all
  ensure
    # clean up
    logger.info("Removing #{outfile}...")
    File.unlink(outfile) if File.file?(outfile)
  end
end
