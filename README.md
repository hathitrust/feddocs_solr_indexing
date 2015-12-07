require 'dotenv'
require_relative 'lib/solrindex'
Dotenv.load

si = SolrIndex.new( ENV['solr_host'], 9035 )
num = si.update
puts "updated #{num}"
