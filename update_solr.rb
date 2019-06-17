require 'dotenv'
require_relative 'lib/solrindex'
Dotenv.load

si = SolrIndex.new( ENV['solr_host'], ENV['solr_port'] )
num = si.update
puts "regrecs updated #{num}"
log = open('log/solr.update.log', 'a')
log.puts "#{ENV['solr_host']}:#{ENV['solr_port']}\t#{num}\t#{DateTime.now.to_s}"
