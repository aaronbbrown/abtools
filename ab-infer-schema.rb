#!/usr/bin/env ruby

require 'pp'
require 'rubygems'
require 'sequel'


# needed for singularize and pluralize methods 
# http://api.rubyonrails.org/classes/ActiveSupport/Inflector.html#method-i-singularize
require 'active_support/inflector'

# for filling in placeholders like '[tablename]_id'
# http://rubygems.org/gems/string_enumerator
require 'string_enumerator' 

# http://www.omninerd.com/articles/Automating_Data_Visualization_with_Ruby_and_Graphviz
# http://rubydoc.info/github/glejeune/Ruby-Graphviz
# http://stackoverflow.com/questions/2941162/ruby-graphviz-binary-tree-record
# https://mailman.research.att.com/pipermail/graphviz-interest/2008q1/005103.html
require 'graphviz'

class Array
  # move <value> to position 0 in the array
  def to_top ( value )
    value ? self.unshift(self.delete(value)) : self
  end
end

# get all columns in the database and 
# return a hash of tablename => [column1, column2,...]
def tables_with_columns( db, table_schema )
  query = "SELECT TABLE_NAME, GROUP_CONCAT(COLUMN_NAME) AS COLUMNS FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? GROUP BY 1"
  ds = db[query, table_schema]
  a = ds.map { |x| [x[:TABLE_NAME], x[:COLUMNS].split(",")] }
  Hash[a]
end

# return a hash of tables and their primary key
# { "table1" => "id", "table2" => "id", ... }
def tables_with_pk(db, table_schema)
  query = "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_KEY = ? AND TABLE_SCHEMA = ?"
  ds = db[query, "PRI", table_schema]
  ds.to_hash(:TABLE_NAME, :COLUMN_NAME)
end

# build a label for the record shaped node
def node_label ( table, fields, pk )
   "#{table}|" + fields.sort.to_top(pk).map {|x| "<#{x}> #{x}" }.join('|') 
end

options = { :pk_pattern  => nil,
            :fk_patterns => ["[table_singular]_id"],
            :database    => "ideeli_production" }

dbparams = { :user     => "ops",
             :password => "c3p0w1nZ",
             :host     => "127.0.0.1",
             :port     => "3333" }

db = Sequel.mysql(dbparams)
 
mappings = {}

pks    = tables_with_pk(db,options[:database])
tables = tables_with_columns(db, options[:database])

tables.each do |k,v|
  se = StringEnumerator.new(:table => k, :table_singular => k.singularize)

  # build a list of possible foreign key names for this table based
  # on the user patterns options[:fk_patterns]
  possible_fks = options[:fk_patterns].map { |x| se.enumerate(x) }.flatten

  # find all tables that actually have 
  # a column that matches one in possible_fks
  # & create an array of hashes
  # [ { "table3" => [ "table1_id", "table2_id" ] },
  #   { "table4" => [ "table2_id" ] } ]
  # 
  fks = possible_fks.map do |fk| 
    Hash[tables.map { |tk,tv| [tk,fk] if tv.include? fk }.compact]
  end.flatten[0]

  # add the foreign key array fks to a hash of key mappings
  # { "table1" => [ 
  #                 { "table3" => [ "table1_id", "table2_id" ] } 
  #               ]
  mappings[k] = fks unless fks.empty?
end

#pp mappings

g = GraphViz.digraph(:G, :type => "digraph")
g[:rankdir] = "LR"
tables.each do |k,v|
  g.add_nodes(k, :shape => "record",
                 :label => node_label(k, v, pks[k]))
end

mappings.each do |parent,children|
  children.each do |child,fields|
    fields.each do |f|
      g.add_edges( {parent => pks[parent]}, {child => f} )
    end
  end
end

g.output(:dot => nil)
g.output(:svg => "/Users/aaron/tmp.svg", :nothugly => true)
