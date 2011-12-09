#!/usr/bin/env ruby

require 'pp'
require 'optparse'
require 'rubygems'
require 'sequel'
require 'socket'

options = { :host     => "localhost",
            :port     => 3306,
            :interval => 60 }


# returns the time that the server is up-to-date as of
def slave_status_lag
  @sequel['SHOW SLAVE STATUS'].first[:Seconds_Behind_Master]
end

def heartbeat_lag (table)
  @sequel["SELECT unix_timestamp(now())-unix_timestamp(ts) AS behind FROM #{table}"].first[:behind]
end

class Integer
  def lpad(num,padstr="0")
    self.to_s.ljust(num, padstr)
  end
end

def sec_to_hms(secs)
  h = (secs/60/60)
  m = (secs/60 % 60)
  s = (secs % 60)
  [h,m,s].map{ |x| x.lpad(2) }.join(":")
end

opts = OptionParser.new
opts.banner = "Usage #{$0} [OPTIONS]"
opts.on("-u", "--user USER",     String,  "MySQL User" )                        { |v| options[:user] = v }
opts.on("-p", "--password PASSWORD", String,  "MySQL Password" )                { |v| options[:password] = v }
opts.on("-H", "--host HOST",     String,  
    "MySQL hostname (default: #{options[:host]})" ) { |v| options[:host] = v }
opts.on("-P", "--port PORT",     Integer, 
    "MySQL port (default #{options[:port]})" )      { |v| options[:port] = v }
opts.on("-s", "--socket SOCKET", Integer, "MySQL socket" )                      { |v| options[:socket] = v }
opts.on("-i", "--interval SECONDS", Integer, 
    "Sleep interval between messages (default #{options[:interval]}s)" ) { |v| options[:socket] = v }
opts.on("-t", "--table TABLE", String, 
    "Heartbeat database.table (must have a timestamp column named ts)") { |v| options[:table] = v }
opts.parse!

@sequel = Sequel.mysql(options)
hostname = Socket.gethostname



loop do
  now = Time.now
  ts = now.strftime("%Y-%m-%d %H:%M:%S")
  sslag = slave_status_lag
  sslag_s = sec_to_hms(sslag)

  if options[:table]
    hblag = heartbeat_lag(options[:table]) 
    hblag_s = sec_to_hms(hblag)
    puts "#{ts} (#{hostname}) - Behind by (SHOW SLAVE STATUS/heartbeat)\t#{sslag_s} / #{hblag_s}"
  else
    puts "#{ts} (#{hostname}) - Behind by #{sslag_s}"
  end

  sleep options[:interval]
end
