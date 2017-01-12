require 'dotenv'
require_relative 'lib/solrindex'
Dotenv.load

si = SolrIndex.new( ENV['solr_host'], 9035 )
num = si.update
puts "regrecs updated #{num}"
log = open('log/solr.update.log', 'a')
log.puts "#{ENV['solr_host']}:9035\t#{num}\t#{DateTime.now.to_s}"
