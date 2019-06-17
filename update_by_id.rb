# quick script to update solr index for individual records
# one regrec id, taken from argv file, at a time. 
require 'dotenv'
require_relative 'lib/solrindex'
Dotenv.load

si = SolrIndex.new( ENV['solr_host'], 9033 )
@mongo_uri = ENV['mongo_host']+':'+ENV['mongo_port']

num_updated = 0
mc = Mongo::Client.new([@mongo_uri], :database => ENV['mongo_db'])

open(ARGV.shift).each do | line |
  reg_id = line.chomp

  rec = mc[:registry].find({"registry_id" => reg_id }).first
  # most of this is just copied from lib/solrindex which is why it should all be refactored
  rec_set = []

  # it has been deprecated
  if rec['suppressed'] or !rec['deprecated_timestamp'].nil?
    # update the deprecated_timestamp, suppressed, deprecated_reason, and successors fields
    doc = {"id":rec['registry_id'], 
            "deprecated_timestamp":{"set":rec['deprecated_timestamp']},
            "suppressed":{"set":rec['suppressed']},
            "deprecated_reason":{"set":rec['deprecated_reason']},
            "successors":{"set":rec['successors']}
          } 
    rec_set << doc
  else
    rec['source_records'] = mc[:source_records].find({"source_id" => 
                                           {'$in' => rec['source_record_ids']}}
                                         ).collect {|s| s['source'].to_json }
    rec['marc_display'] = rec['source_records'][0]
    rec['id'] = rec['registry_id']
    rec.delete("_id")
    #the sorts can't be multivalue
    ['author_sort', 'pub_date_sort', 'title_sort'].each do | sort |
      if rec[sort] 
        rec[sort] = rec[sort][0] 
      end
    end
    if rec['pub_date']
      rec['pub_date_sort'] = rec['pub_date'][0]
    end
    rec_set << rec
  end 

  num_updated += 1
  si.insert rec_set
  rec_set = []
end

puts "num updated: #{num_updated}"
