module ABTools
  def self.read_my_cnf ( filename )
    h = {}
    ini = IniFile.new(filename)
    h[:host]     ||= ini['client']['host']
    h[:user]     ||= ini['client']['user']
    h[:password] ||= ini['client']['password']
    h[:port]     ||= ini['client']['port']
    h[:socket]   ||= ini['client']['socket']
    h
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

end
