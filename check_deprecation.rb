require 'dotenv'
require_relative 'lib/solrindex'
Dotenv.load

si = SolrIndex.new( ENV['solr_host'], 9035 )

num_needing_update = 0

fout = open('ids_needing_update.txt', 'a')

open(ARGV.shift).each do | line |
  reg_id = line.chomp

  doc = si.document(reg_id, "suppressed")
  if doc.nil?
    puts "nil: #{reg_id}"
  elsif !doc["suppressed"]
    fout.puts reg_id
    num_needing_update += 1
  end
end
