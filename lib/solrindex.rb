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

  def insert(documents)
    begin
      chunk = '['+documents.join(',')+']'
      resp = @client.post @solr_uri, chunk, "content-type"=>"application/json"
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
    recs = self.recs_modified_after(@last_updated).each
    queue = Queue.new

    thread_pool = (0...8).map do 
      Thread.new do
        begin 
          chunk_size = 20
          rec_set = []
          chunk = ''
          mc = Mongo::Client.new([@mongo_uri], :database => ENV['mongo_db'])
          while !queue.empty?
            rec = queue.pop
            rec['source_records'] = mc[:source_records].find({"source_id" => 
                                                   {'$in' => rec['source_record_ids']}}
                                                 ).collect {|s| s['source_blob']}
            rec['id'] = rec['registry_id']
            rec.delete("_id")
            rec_set << rec.to_json
            if rec_set.count % chunk_size == 0
              self.insert rec_set
              rec_set = []
            end
          end
          if rec_set.count > 0
            self.insert rec_set
            rec_set = []
          end
        rescue ThreadError
          puts "threadError!"
          STDOUT.flush
        end
      end
    end
    self.recs_modified_after(@last_updated).each { |r| queue << r }
    thread_pool.map(&:join)
    return recs.count
  end

  def recs_modified_after(start_time)
    recs = @mc[:registry].find({"last_modified" => {'$gt' => start_time}})
    return recs
  end
     
end

