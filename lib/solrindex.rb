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
    # temporarily get records that are missing
    #@current_recs = []
    #File.open("/htapps-dev/jstever.babel/jira_tickets/2034_solr_db/regids_needing_updating_03-06.txt").each do |line|
    #  @current_recs << line.chomp
    #end
  end

  def insert(documents)
    begin
      #chunk = '['+documents.join(',')+']'
      resp = @client.post @solr_uri, documents.to_json, "content-type"=>"application/json"
      puts "status code:#{resp.status_code}"
      if resp.status_code == 400 or resp.status_code == 404
        #PP.pp documents.collect {|s| s['id']}
        #STDOUT.flush
        raise resp.status_code.to_s
      elsif resp.status_code == 200
        STDOUT.flush
      end
      return resp
    rescue Errno::ECONNREFUSED => e
      puts e.message 
      #PP.pp documents.collect {|s| s['id'] }
      STDOUT.flush
      exit
    rescue => error
      PP.pp error
      documents.each do |d|
        if d['id'].nil?
          PP.pp d
          exit
        end
      end
      PP.pp documents.collect {|s| s['id'] }
      PP.pp resp
      STDOUT.flush
      #sleep(1)
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
      # next if @current_recs.include? r['registry_id']
      #puts r['registry_id']
      queue << r['registry_id'] 
    end
    reg_count =  queue.length

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

            # it has been deprecated
            if rec['suppressed'] or !rec['deprecated_timestamp'].nil?
              # update the deprecated_timestamp, suppressed, deprecated_reason, and successors fields
              if rec['deprecated_timestamp']
                rec['deprecated_timestamp'] = rec['deprecated_timestamp'].to_s.sub(/ UTC/, 'Z').sub(/ /, 'T')
              end
              doc = {'id':rec['registry_id'], 
                      'deprecated_timestamp':{"set":rec['deprecated_timestamp']},
                      'suppressed':{"set":rec['suppressed']},
                      'deprecated_reason':{"set":rec['deprecated_reason']},
                      'successors':{"set":rec['successors']}
                    }
              rec_set << doc
              #resp = @client.post @solr_uri, doc.to_json, "content-type"=>"application/json"
              #puts "deprecated: #{rec['registry_id']}"
            else
              rec['source_records'] = mc[:source_records].find({"source_id" => 
                                                     {'$in' => rec['source_record_ids']}}
                                                   ).collect {|s| s['source'].to_json }
              rec['marc'] = rec['source_records'][0]
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
    return reg_count
  end

  def recs_modified_after(start_time)
    recs = @mc[:registry].find({"last_modified" => {'$gt' => start_time}})
    #recs = @mc[:registry].find({"oclc" => 1286390})
    #recs = @mc[:registry].find({})
    return recs
  end

  def document(reg_id, fl="*" )  
    solr_uri = "http://#{@host}:#{@port}/usfeddocs/collection1/select?id=#{reg_id}&fl=#{fl}&wt=json&qt=document"
    resp = @client.get solr_uri
    JSON.parse(resp.body)["response"]["docs"][0]
  end
    
end

