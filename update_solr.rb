require 'dotenv'
require_relative 'lib/solrindex'
Dotenv.load

si = SolrIndex.new( ENV['solr_host'], 9035 )
num = si.update
puts "updated #{num}"
log = open('log/solr.update.log', 'a')
log.write("#{ENV['solr_host']}:9035\t#{num}\t#{DateTime.now.to_s}")
