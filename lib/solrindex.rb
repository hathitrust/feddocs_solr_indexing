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
      #chunk = '['+documents.join(',')+']'
      resp = @client.post @solr_uri, documents.to_json, "content-type"=>"application/json"
      if resp.status == "400"
        raise "400"
      elsif resp.status == "200"
        PP.pp resp
	STDOUT.flush
      end
      return resp
    rescue => error
      PP.pp error
      PP.pp documents.collect {|s| s['registry_id'] }
      PP.pp resp
      STDOUT.flush
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
    queue = Queue.new
    self.recs_modified_after(@last_updated).each do |r|
      #puts r['registry_id']
      queue << r['registry_id'] 
    end
    count =  queue.length

    thread_pool = (0...4).map do 
      Thread.new do
        begin 
          chunk_size = 10
          rec_set = []
          chunk = ''
          mc = Mongo::Client.new([@mongo_uri], :database => ENV['mongo_db'])
          while !queue.empty?
            rec_id = queue.pop
            rec = mc[:registry].find({"registry_id" => rec_id }).first
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
    thread_pool.map(&:join)
    return count 
  end

  def recs_modified_after(start_time)
    recs = @mc[:registry].find({"last_modified" => {'$gt' => start_time}})
    return recs
  end
     
end

