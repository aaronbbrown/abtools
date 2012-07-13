#!/usr/bin/env ruby

require 'pp'
require 'optparse'
require 'rubygems'
require 'sequel'
require 'logger'

class Hash
  def merge_keys ( keys )
    Hash[*self.select { |k,v| keys.include? k }.flatten]
  end
end

module ABTools

  class SQLChunker
    attr_reader :pk_col, :source_db, :source_tbl, :dest_db, :dest_tbl, :min_id, :max_id, :last_id
    def initialize ( h )
      @source_db  = h[:source_db]
      @source_tbl = h[:source_tbl]
      @dest_db    = h[:dest_db]
      @dest_tbl   = h[:dest_tbl]
      @chunksize  = h[:chunksize]
      @logger     = h[:logger] || Logger.new(STDOUT)

      params = h.merge_keys([:host,:user,:password,:port,:socket])
      @sequel = Sequel.mysql(params)
      @sequel.logger = @logger

      @pk_col = get_primary_key_column
      throw "No primary key" if @pk_col.empty?

      @min_id,@max_id = get_id_bounds
    end

    def get_primary_key_column 
      query = "SELECT k.column_name FROM information_schema.table_constraints t JOIN information_schema.key_column_usage k USING(constraint_name,table_schema,table_name) WHERE t.constraint_type='PRIMARY KEY' AND t.table_schema=? AND t.table_name=?;"
    
      ds = @sequel[query,@source_db,@source_tbl]
      ds.first[:column_name]
    end

    def get_id_bounds
      query = "SELECT min(`#{@pk_col}`) AS min_id, max(`#{@pk_col}`) AS max_id  FROM `#{@source_db}`.`#{@source_tbl}`"
      ds = @sequel[query]
      [ds.first[:min_id],ds.first[:max_id]]
    end

    def run
      @last_id = 0
      (@min_id..@max_id).step(@chunksize) do |n|
        endid = [(n+@chunksize-1),@max_id].min
        query = "INSERT INTO `#{@dest_db}`.`#{@dest_tbl}` SELECT * FROM `#{@source_db}`.`#{@source_tbl}` WHERE `#{@pk_col}` BETWEEN ? AND ?"
        ds = @sequel[query,n,endid]
        @last_id = ds.insert
        @logger.info "Last id inserted #{@last_id}"
      end

    end
  end
end 

options = { :host       => "localhost",
            :user       => nil,
            :password   => nil,
            :port       => 3306,
            :socket     => nil,
            :source_tbl => nil,
            :source_db  => nil,
            :dest_tbl   => nil,
            :dest_db    => nil,
            :chunksize  => 10000 }


opts = OptionParser.new
opts.banner = "Usage #{$0} [OPTIONS]"
opts.on("-u", "--user USER", String,  "MySQL User" )  do |v|
  options[:user] = v 
end
opts.on("-p", "--password PASSWORD", String,  "MySQL Password" )  do |v| 
  options[:password] = v 
end
opts.on("-P", "--port PORT", Integer, "MySQL port (default #{options[:port]})" )  do |v|
  options[:port] = v 
end
opts.on("-H", "--host HOST", String,  "MySQL hostname (default: #{options[:host]})" )  do |v|
  options[:host] = v 
end
opts.on("-c", "--chunk SIZE", Integer, "Chunk size (default #{options[:chunksize]})" )  do |v|
  options[:chunksize] = v 
end
opts.on("-s", "--source-db DB", String, "Source Database") do |v|
  options[:source_db] = v
end
opts.on("-S", "--source-table TABLE", String, "Source Table") do |v|
  options[:source_tbl] = v
end
opts.on("-d", "--dest-db DB", String, "Destination Database") do |v|
  options[:dest_db] = v
end
opts.on("-D","--dest-table TABLE", String, "destination table") do |v|
  options[:dest_tbl] = v
end
opts.on("-h", "--help",  "this message") { puts opts; exit 1}

opts.parse!
pp options
logger = Logger.new(STDOUT)
logger.level = Logger::Info
options[:logger] = logger

sc = ABTools::SQLChunker.new options

logger.info "Primary key column: #{sc.pk_col} #{sc.min_id}..#{sc.max_id}"
sc.run 
logger.info "DONE: Last inserted ID: #{sc.last_id}"
