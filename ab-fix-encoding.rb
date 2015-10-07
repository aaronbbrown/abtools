#!/usr/bin/env ruby

require 'rubygems'
require 'sequel'
require 'logger'
require 'optparse'
require 'pp'

def column_def_from_create_table ( db, table_name, column_name )
  result = db["SHOW CREATE TABLE `#{table_name}`"].first
  return nil unless result[:'Create Table']
  result[:'Create Table'].match(/^\s*(`#{column_name}`.*?),?$/).captures.first
end

def columns_to_alter ( db, database_name, from_encoding )
  db[:information_schema__columns].
    select(:columns__table_name, :column_name, :column_type).
    join(:information_schema__tables,
         [:columns__table_name => :tables__table_name,
          :columns__table_schema => :tables__table_schema,
          :tables__table_type => 'BASE TABLE' ]).
    where(:columns__table_schema => database_name,
          :character_set_name => from_encoding).
    exclude(:character_set_name => nil).
    order(:columns__table_name,:columns__column_name).all
end

def database_to_alter ( db, database_name, from_encoding)
  results = db[:information_schema__schemata].
             select(:schema_name, :default_character_set_name).
             where(:schema_name => database_name,
                   :default_character_set_name => from_encoding).first
  results[:schema_name] if results
end

def tables_to_alter ( db, database_name, from_encoding )
  db[:information_schema__tables].
    join(:information_schema__collation_character_set_applicability, :collation_name => :table_collation).
    select(:table_name).
    where(:table_schema => database_name,
          :character_set_name => from_encoding).map { |row| row[:table_name] }
end
@logger = Logger.new $stderr
@logger.level = Logger::DEBUG

options = {
  :user          => nil,
  :password      => nil,
  :host          => '127.0.0.1',
  :port          => 3306,
  :database      => nil,
  :to_encoding   => nil,
  :from_encoding => nil,
  :skip_binlog   => false
}

opts = OptionParser.new
opts.banner = "Usage #{$0} [OPTIONS]"
opts.on("-u", "--user USER",           String, "MySQL User" )  { |v|  options[:user] = v }
opts.on("-p", "--pass PASSWORD",       String, "MySQL Password" )  { |v|  options[:password] = v }
opts.on("-P", "--port PORT",           Integer,"MySQL port (default #{options[:port]})" )  { |v| options[:port] = v }
opts.on("-H", "--host HOST",           String, "MySQL hostname (default: #{options[:host]})" )  { |v| options[:host] = v }
opts.on("-d", "--database DATABASE",   String, "MySQL Database (default: #{options[:database]})" )  { |v| options[:database] = v }
opts.on("-f", "--from ENCODING",       String, "Convert all tables from this encoding" ) { |v| options[:from_encoding] = v }
opts.on("-t", "--to ENCODING",         String, "Convert all tables to this encoding" )   { |v| options[:to_encoding] = v }
opts.on('-c', '--collation COLLATION', String, 'Convert all tables to use this collaction') { |v| options[:collation] = v }
opts.on('-B', '--skip-binlog',                 'Do not write to binlog') { |v| options[:skip_binlog] = true }
opts.on("-h", "--help",  "this message") { puts opts; exit 1}
opts.parse!

unless options[:from_encoding] && options[:to_encoding]
  puts "--to and --from are required parameters!"
  puts
  puts opts
  exit 1
end

db = Sequel.connect(
  :adapter       => 'mysql',
  :sql_log_level => :debug,
  :logger        => @logger,
  :user          => options[:user],
  :password      => options[:password],
  :host          => options[:host] || 'localhost',
  :port          => options[:port] || 3306,
  :database      => options[:database]
)

sql, pre_sql, post_sql = [], [], []

db_cols = columns_to_alter(db, options[:database], options[:from_encoding])
table_list = db_cols.group_by { |row| row[:table_name] }.keys
table_list += tables_to_alter(db, options[:database], options[:from_encoding])
collation_substring = options[:collation] ? " COLLATE #{options[:collation]}" : nil

if options[:skip_binlog]
  pre_sql << 'SET sql_log_bin = 0'
end

sql << "-- base table alters"
table_list.sort.uniq.each do |table_name|
  sql << "ALTER TABLE `#{table_name}` CHARACTER SET #{options[:to_encoding]}#{collation_substring}"
end

pre_sql << "-- fix the database itself"
db_to_alter = database_to_alter(db,options[:database],options[:from_encoding])
if db_to_alter
  pre_sql << "ALTER DATABASE `#{db_to_alter}` CHARACTER SET #{options[:to_encoding]}#{collation_substring}"
end

sql << "-- fix the individual columns"
db_cols.each do |row|
  original_column_def = column_def_from_create_table(db, row[:table_name], row[:column_name])
  next unless original_column_def
  alter_sql_prefix = "ALTER TABLE `#{row[:table_name]}` MODIFY"
  sql << "#{alter_sql_prefix} #{original_column_def}" # CHARACTER SET #{options[:to_encoding]}"
end

sql = pre_sql + sql
sql += post_sql
puts sql.uniq.join(";\n")
