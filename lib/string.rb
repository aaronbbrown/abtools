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

