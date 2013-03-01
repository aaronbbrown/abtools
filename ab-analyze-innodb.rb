#!/usr/bin/env ruby

require 'pp'
require 'rubygems'
require 'sequel'
require 'logger'
require 'optparse'

module TableAnalyzer
  class Table
# Public: Returns the String name of the schema
    attr_reader :schema
# Public: Returns the String name of the table    
    attr_reader :table

# Public: Initialize a table
# 
# h - A Hash of :schema, :table, :db, and :logger
#     :schema  - The String name of the database schema
#     :table   - The String name of the table
#     :db      - The Sequel::Database object
#     :logger  - The Logger object
    def initialize ( h )
      @schema ||= h[:schema]
      @table  ||= h[:table]
      @db     ||= h[:db]
      @logger = h[:logger] || Logger.new("/dev/null")
    end

# Public: return a canonically formatted name for the table
#
# Examples
#   formatted_name
#   # => `foo_db`.`bar_tbl`
    def formatted_name
      "`#{@schema}`.`#{@table}`"
    end

# Public: Loop until the Table is unused (according to the MySQL PROCESSLIST)
# or max_tries has been exceeded.
#
# max_tries  - Number of iterations before considered a failure
# sleep_time - Number of seconds to sleep inbetween failures
# block      - The code to execute once the Table is found to be unused
#
# Examples
# 
#   tbl.unused(10,5) do 
#     tbl.analyze
#   end
#
#   Returns false if the Number of iterations was exceeded, true otherwise.
    def unused (max_tries, sleep_time)
      max_tries.times do |i|
        if in_use? 
          @logger.warn "Attempt #{i} of #{max_tries}.  #{formatted_name} is in use.  Sleeping for #{sleep_time} secs..."
          sleep sleep_time 
        else
          yield
          return true
        end
      end
      @logger.warn "Max tried reached for #{formatted_name}"
      false
    end

# Public: Find out if the Table is in use according the MySQL PROCESSLIST
#
# Examples
#
#   tbl.in_use?
#   # => true
#
# Returns true if the Table is in use, false otherwise
    def in_use?
      table_in_use_query = <<-SQL
        SELECT info 
        FROM INFORMATION_SCHEMA.PROCESSLIST 
        WHERE DB = ? AND INFO <> 'null'
      SQL
      @db[table_in_use_query, @schema].each do |row|
        return true if row[:info] =~ /\b`?(#{@schema}\.`?)?`?#{@table}`?\b/i
      end
      false
    end

# Public: Execute an ANALYZE TABLE statement on the Table
#
# local - Whether to add the LOCAL keyword to ANALYZE TABLE (default: false)
# 
# Returns nothing.
    def analyze ( local = false )
      @logger.info "analyzing #{formatted_name}..."
      local_modifier = local ? "LOCAL " : ""
      analyze_query = "ANALYZE #{local_modifier} TABLE #{formatted_name}"
      @db.run analyze_query
    end
  end
end


class IniFile
  def initialize(filename)
    @sections = {}
    file = File.open(filename, 'r')
    key = ""
    file.each do |line|
      if line =~ /^\[(.*)\]/
        key = $1
        @sections[key] = Hash.new
      elsif line =~ /^(.*?)\s*\=\s*(.*?)\s*$/
        if @sections.has_key?(key)
          @sections[key].store($1, $2)
        end
      end
    end
    file.close
  end

  def [] (key)
    return @sections[key]
  end

  def each
    @sections.each do |x,y|
      yield x,y
    end
  end

  def regex_filter! ( section_filter, key_filter  )
    if section_filter
      @sections = @sections.select { |k,v| k =~ section_filter }
    end
    if key_filter
      newsections = {}
      @sections.each do |section,entry|
        newsections[section]  = entry.select { |k,v| k =~ key_filter }
      end
      @sections = newsections
    end
    return self
  end

  def to_h
    return @sections
  end
end

# Convert an Array of db.tbl names into an Array of Hashes 
# that looks like INFORMATION_SCHEMA.TABLES.
#
# a - The array of db.tbl names to be hashified
#
# Examples
# 
#   tbl_a_to_h( %w[db1.tbl1 db2.tbl2] )
#   #  => [{:table_schema=>"db1", :table_name=>"tbl1"}, 
#          {:table_schema=>"db2", :table_name=>"tbl2"}] 
#
#  Returns the hash format
def tbl_a_to_h ( a )
  a.map do |x| 
    db_tbl = x.split('.')
    { :table_schema => db_tbl[0],
      :table_name   => db_tbl[1] }
  end
end

# Return a snippet of a SQL WHERE clause to only retrieve
# a list of specified tables
#
# tables - The Array of Hashes as returned by tbl_a_to_h
#
# Examples
#
#   tbl_list_where( [{ :table_schema => 'db1', :table_name => 'tbl1'},
#                    { :table_schema => 'db2', :table_name => 'tbl2'} )
#   # => "(table_schema = 'db1' AND table_name = 'tbl1') OR
#         (table_schema = 'db2' AND table_name = 'tbl2')"
#
# Returns a String that is a WHERE clause without the WHERE keyword.
def tbl_list_where ( tables )
  tables.map do |x|
    "(table_schema = '#{x[:table_schema]}' AND table_name = '#{x[:table_name]}')"
  end.join(" OR ")
end

def read_defaults_file ( mysql_opts, ini )
  h = {}
  h[:host]     ||= ini['client']['host']
  h[:user]     ||= ini['client']['user']
  h[:password] ||= ini['client']['pass']
  h[:port]     ||= ini['client']['port']
  h[:socket]   ||= ini['client']['socket']
  h
end

mysql_opts = { :user     => 'root',
               :password => '',
               :host     => 'localhost',
               :port     => 3306,
               :socket   => '' }

options = { :local   => false,
            :verbose => false, 
            :sleep   => 0 }

opts = OptionParser.new
opts.banner = "Usage #{$0} [OPTIONS]"
opts.on("-u", "--user USER", String,  "MySQL User" )  do |v|
  mysql_opts[:user] = v
end
opts.on("-p", "--password PASSWORD", String,  "MySQL Password" )  do |v|
  mysql_opts[:password] = v
end
opts.on("-P", "--port PORT", Integer, "MySQL port (default #{mysql_opts[:port]})" )  do |v|
  mysql_opts[:port] = v
end
opts.on("-H", "--host HOST", String,  "MySQL hostname (default: #{mysql_opts[:host]})" )  do |v|
  mysql_opts[:host] = v
end
opts.on("-s", "--socket SOCKET", String,  
    "MySQL socket (default: #{mysql_opts[:socket]})" )  do |v|
  mysql_opts[:socket] = v
end
opts.on("-S", "--sleep SECONDS", Integer,
        "Sleep NUMBER seconds between each table (default: #{options[:sleep]})" ) do |v|
  options[:sleep] = v
end
opts.on("-t", "--tables TABLES", Array, 
    "Comma separated list of db.table to analyze") do |v|
  options[:tables] = v
end
opts.on("-l", "--local", 
    "Add LOCAL keyword to ANALYZE TABLE statement (default #{options[:local]})") do |v|
  options[:local] = v
end
opts.on("--defaults-file FILE", String,
    "Specify a my.cnf file that contains user,pass,etc under the [client] header") do |v|
  ini = IniFile.new(v).to_h
  unless ini['client']
    STDERR.puts "No [client] header found" 
    exit 1
  end
  mysql_opts.merge!(read_defaults_file(mysql_opts, ini))
end
opts.on("-v", "--verbose", "Verbose output") do |v|
  options[:verbose] = v
end
opts.on("-h", "--help",  "this message") { puts opts; exit 1}

unless opts.parse!
  puts opts
  exit 1
end

logger = nil 
failed = []
where  = ""

db = Sequel.mysql( 'INFORMATION_SCHEMA', mysql_opts )
logger = Logger.new(STDOUT)
logger.level = options[:verbose] ? Logger::DEBUG : Logger::INFO
db.loggers << logger
db.sql_log_level = :debug

if options[:tables] && !options[:tables].empty?
  tbl_h = tbl_a_to_h(options[:tables])
  where = tbl_list_where(tbl_h)
  if where
    where = "AND (#{where})"
  end
end

tables_query = <<-SQL
SELECT table_schema, table_name 
FROM information_schema.tables 
WHERE engine = 'InnoDB' #{where}
SQL

ds = db[tables_query]
ds.each do |row|
  tbl = TableAnalyzer::Table.new( :schema => row[:table_schema],
                                  :table  => row[:table_name],
                                  :db     => db,
                                  :logger => logger)
  result = tbl.unused(12, 5) do
    tbl.analyze options[:local] 
  end

  if options[:sleep] > 0
    logger.info "Sleeping for #{options[:sleep]} seconds"
    sleep(options[:sleep]) 
  end

  failed << tbl.formatted_name unless result
end

if !failed.empty?
  puts "There were errors analyzing the following tables:"
  pp failed
  exit 1
end
