require 'httpclient'
require 'pp'
require 'date'
require 'json'
require 'mongo'

class SolrIndex 
  attr_reader :client, :host, :port, :last_upated, :solr_uri
 
  def initialize(host, port)
    @client = HTTPClient.new
    @mongo_uri = ENV['mongo_host']+':'+ENV['mongo_port']
    Mongo::Logger.logger.level = ::Logger::FATAL
    @mc = Mongo::Client.new([@mongo_uri], :database => ENV['mongo_db'])
    @host = host
    @port = port
    @solr_uri = "http://#{@host}:#{@port}#{ENV['solr_path']}"
    @last_updated = self.get_last_updated 
  end

  def insert(document)
    begin
      document.delete("_id")
      document['id'] ||= document['registry_id']
      resp = @client.post @solr_uri, "[#{document.to_json}]", "content-type"=>"application/json"
      if resp.status == "400"
        raise "400"
      end
      return resp
    rescue
      puts "Failed!"
      PP.pp resp
      sleep(1)
      retry
    end
  end

  def get_last_updated
    #don't judge me
    lu = `grep '#{@host}:#{@port}' #{ENV['log_file']} | tail -1`
    return Time.parse(lu.split(/\t/)[2]).to_time
  end

  def update
    @update_start_time = Time.now
    recs = self.recs_modified_after @last_updated
    recs.each do | rec |
      rec['source_records'] = @mc[:source_records].find({"source_id" => 
                                                   {'$in' => rec['source_record_ids']}}
                                                 ).collect {|s| s['source'].to_json}
      rec['id'] = rec['registry_id']
      self.insert rec
    end
    return recs.count
  end

  def recs_modified_after(start_time)
    recs = @mc[:registry].find({"lastModified" => {'$gt' => start_time}})
    return recs
  end
     
end

