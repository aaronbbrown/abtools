#!/usr/bin/env ruby

# script to copy an entire database from one schema to another

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

# get tables from source database
def get_tables ( sequel, db )
  query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'"
  ds = sequel[query,db]
  ds.map { |x| x[:TABLE_NAME] }.sort
end

def get_views ( sequel, db )
  query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'VIEW'"
  ds = sequel[query,db]
  ds.map { |x| x[:TABLE_NAME] }.sort
end

def get_db_def ( sequel, db )
  query = <<SQL
SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME 
FROM INFORMATION_SCHEMA.SCHEMATA
WHERE SCHEMA_NAME = ?
SQL
  sequel[query,db].first
end

def create_db ( sequel, source_db, dest_db )
  source_def = get_db_def(sequel, source_db)
  query = <<SQL
CREATE DATABASE `#{dest_db}` 
DEFAULT CHARACTER SET #{source_def[:DEFAULT_CHARACTER_SET_NAME]}
DEFAULT COLLATE #{source_def[:DEFAULT_COLLATION_NAME]}
SQL
  sequel[query].all
end

def quote_table ( db, table )
  "`#{db}`.`#{table}`"
end

def create_table (sequel, table, source_db, dest_db )
  query = <<SQL
CREATE TABLE #{quote_table(dest_db,table)} 
LIKE #{quote_table(source_db,table)}
SQL
  sequel[query].all
end

def copy_table ( sequel, table, source_db, dest_db )
  create_table(sequel, table, source_db, dest_db)
  query = <<SQL
INSERT INTO #{quote_table(dest_db,table)}
SELECT * FROM #{quote_table(source_db,table)}
SQL
  sequel[query].all
end

def get_view_def ( sequel, view, db )
  query = "SHOW CREATE VIEW #{quote_table(db,view)}"
  sequel[query].first
end

def copy_view ( sequel, view, source_db, dest_db )
  query = rewrite_create_view(sequel, view, source_db, dest_db)
  sequel[query].all
end

def rewrite_create_view ( sequel, view, source_db, dest_db)
  view_def = get_view_def(sequel, view, source_db)
  create_sql = view_def[:'Create View']
#  VIEW `staff_list` AS
  create_sql.sub!(" VIEW `#{view}` AS ", " VIEW #{quote_table(dest_db,view)} AS ")
# replace all occurrences of the db reference 
  create_sql.gsub("`#{source_db}`.", "`#{dest_db}`.")
end

options = { :dsn             => nil,
            :source_db       => nil,
            :destination_db  => nil,
            :local           => nil,
          }

opts = OptionParser.new
opts.banner = "Usage: #{$0} [OPTIONS]"
opts.on("-d", "--dsn DSN", String, "Connection DSN") do |v|
  options[:dsn] = v.to_h
end
opts.on("--source-db DATABASE", String, "Source database") do |v|
  options[:source_db] = v
end
opts.on("--destination-db DATABASE", String, "Destination database") do |v|
  options[:destination_db] = v
end
opts.on("-l", "--local", "Don't log to binary log") do |v|
  options[:local] = v
end
opts.on("-h", "--help", "This message") { puts opts; exit 1 }
opts.parse!

# validate some assumptions
unless options[:dsn] &&
       options[:source_db] &&
       options[:destination_db]
  puts opts
  exit 1
end

if options[:source_db].downcase == options[:destination_db].downcase
  STDERR.puts "source database and destination database cannot be the same"
  puts opts
  exit 1
end

logger = Logger.new(STDOUT)
logger.level = Logger::DEBUG

sequel = Sequel.mysql( options[:dsn]['d'], {
    :host     => options[:dsn]['h'] || 'localhost',
    :user     => options[:dsn]['u'],
    :password => options[:dsn]['p'],
    :port     => options[:dsn]['P'] || 3306,
    :socket   => options[:dsn]['s']
    } )
sequel.loggers << logger

if options[:local]
  sequel["SET SQL_LOG_BIN=0"].update
end

create_db(sequel, options[:source_db], options[:destination_db]) 
tables = get_tables(sequel, options[:source_db])
tables.each do |table|
  copy_table(sequel, table, options[:source_db], options[:destination_db])
end

views = get_views(sequel, options[:source_db])
views.each do |view|
  copy_view(sequel, view, options[:source_db], options[:destination_db])
end
