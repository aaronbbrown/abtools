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
        :host     => h[:host]      || 'localhost',
        :socket   => h[:socket],
        :user     => h[:user],
        :password => h[:password],
        :port     => h[:port],
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

    def stats
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

    def common_size_stats ( key_prefix, row )
      stats = {}
      stats["#{key_prefix}.reserved_bytes"] = to_bytes(row[:reserved])   unless row[:reserved].to_i   == 0
      stats["#{key_prefix}.data_bytes"]     = to_bytes(row[:data])       unless row[:data].to_i       == 0
      stats["#{key_prefix}.index_bytes"]    = to_bytes(row[:index_size]) unless row[:index_size].to_i == 0
      stats["#{key_prefix}.unused_bytes"]   = to_bytes(row[:unused])     unless row[:unused].to_i     == 0
      stats
    end

    # this is hacked from the built-in sp_spaceused function which
    # returns multiple resultsets and is incompatible with
    # pretty much every ruby library in existence.
    def spaceused
       sql = <<SQL
select sum(convert(bigint,case when status & 64 = 0 then size else 0 end)) AS dbsize,
       sum(convert(bigint,case when status & 64 <> 0 then size else 0 end)) AS logsize
from dbo.sysfiles
SQL
      rs = @conn[sql].first
      dbsize  = rs[:dbsize]
      logsize = rs[:logsize]

      sql = <<SQL
select sum(a.total_pages) AS reservedpages,
       sum(a.used_pages) AS usedpages,
       sum(
        CASE
          -- XML-Index and FT-Index-Docid is not considered "data", but is part of "index_size"
          When it.internal_type IN (202,204) Then 0
          When a.type <> 1 Then a.used_pages
          When p.index_id < 2 Then a.data_pages
          Else 0
        END
      ) AS pages
from sys.partitions p join sys.allocation_units a on p.partition_id = a.container_id
left join sys.internal_tables it on p.object_id = it.object_id
SQL
      rs = @conn[sql].first
      reservedpages = rs[:reservedpages]
      usedpages     = rs[:usedpages]
      pages         = rs[:pages]

      sql = <<SQL
select database_name = db_name(),
       database_size = ltrim(str((convert (dec (15,2),?) + convert (dec (15,2),?))
      * 8192 / 1048576,15,2) + ' MB'),
    'unallocated space' = ltrim(str((case when ? >= ? then
      (convert (dec (15,2),?) - convert (dec (15,2),?))
      * 8192 / 1048576 else 0 end),15,2) + ' MB')
SQL
      rs = @conn[sql,dbsize,logsize,dbsize,reservedpages,dbsize,reservedpages].first
      stats = { :database_name => rs[:database_name],
                :database_size => rs[:database_size] }

      sql = <<SQL
select
    reserved = ltrim(str(? * 8192 / 1024.,15,0) + ' KB'),
    data = ltrim(str(? * 8192 / 1024.,15,0) + ' KB'),
    index_size = ltrim(str((? - ?) * 8192 / 1024.,15,0) + ' KB'),
    unused = ltrim(str((? - ?) * 8192 / 1024.,15,0) + ' KB')
SQL
      rs = @conn[sql,reservedpages,pages,usedpages,pages,reservedpages,usedpages].first
      stats.merge( :reserved   => rs[:reserved],
                   :data       => rs[:data],
                   :index_size => rs[:index_size],
                   :unused     => rs[:unused] )
    end

    # return some stats about the database itself
    def db_stats(db)
      stats = {}
      @conn.execute("USE [#{db}]")
      row = spaceused
      key_prefix = [ @prefix, row[:database_name] ].compact.join('.')

      stats["#{key_prefix}.database_size_bytes"] = to_bytes(row[:database_size_bytes]) unless row[:database_size_bytes].to_i == 0
      stats.merge common_size_stats(key_prefix,row)
    end

    def databases ( exclude_dbs = [])
      base_query = 'SELECT name FROM sys.databases'
      if exclude_dbs.empty?
        ds = @conn[base_query]
      else
        ds = @conn["#{base_query} WHERE name NOT IN ?", exclude_dbs]
      end
      ds.map { |x| x[:name] }
    end

    def tbl_stats_for_db ( db )
      stats = {}
      begin
        @conn.execute("USE [#{db}]")
        space_used = @conn["EXEC sp_MSforeachtable 'EXEC sp_spaceused ''?'''"].all
        space_used.each do |row|
          key_prefix = [ @prefix, db,
                         row[:name] ].compact.join('.')
          stats["#{key_prefix}.total_bytes"]    = to_bytes(row[:data]) + to_bytes(row[:index_size])
          stats["#{key_prefix}.rows"]           = row[:rows].to_i unless row[:rows].to_i == 0
          stats.merge!(common_size_stats(key_prefix,row))
        end
      rescue
      end
      stats
    end

    def stats
      stats = {}
      exclude_dbs = %w[tempdb master]
      databases(exclude_dbs).each do |db| 
        stats.merge! tbl_stats_for_db(db) 
      end
      databases(%w[master model msdb]).each { |db| stats.merge! db_stats(db) }
      stats
    end
  end
end


