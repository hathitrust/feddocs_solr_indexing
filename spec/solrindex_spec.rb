require 'solrindex'
require 'dotenv'
require 'securerandom'
require 'pp'
require 'mongo'

Dotenv.load

RSpec.describe SolrIndex, "#client" do
  it "has the given db_name, host and port" do
    si = SolrIndex.new( ENV['solr_host'], 9034 )
    expect(si.host).to eq ENV['solr_host']
    expect(si.port).to eq 9034
  end

end

RSpec.describe SolrIndex, "document" do
  it "gets a document by id" do
    id = "ee5efb31-5c36-4f2b-8a0f-5b9709b021b7"
    si = SolrIndex.new(ENV['solr_host'], 9034 )
    doc = si.document(id)
    expect(doc["pub_date"][0]).to eq('1998')
  end

  it "gets one field from a document" do 
    id = "ee5efb31-5c36-4f2b-8a0f-5b9709b021b7"
    si = SolrIndex.new(ENV['solr_host'], 9034 )
    doc = si.document(id, "pub_date")
    expect(doc["pub_date"][0]).to eq('1998')
  end
end  

RSpec.describe SolrIndex, "insert" do
  it "inserts new documents" do
    test_id = SecureRandom.uuid()
    document = {"id"=>"#{test_id}", "title_display"=>"testing"}
    
    si = SolrIndex.new( ENV['solr_host'], 9034 )
    resp = si.insert( [document] )
    expect(resp.status_code).to eq 200
  end 

  it "inserts existing documents from Mongo" do
    db = Mongo::Client.new([ENV['mongo_host']+':'+ENV['mongo_port']], 
                            :database => ENV['mongo_db'])
    db['registry'].find().limit(1).each do | rec |
      si = SolrIndex.new( ENV['solr_host'], 9034 )
      rec['id'] = rec['registry_id']
      rec.delete("_id")
      resp = si.insert( [rec] )
      expect(resp.status_code).to eq 200
    end
  end
end

RSpec.describe SolrIndex, "#update" do
  before(:each) do
    @si = SolrIndex.new( ENV['solr_host'], 9034 )
  end

  it "gets recs that need updating" do
    expect(@si.recs_modified_after(Time.now.utc).count()).to eq 0
  end 

  it "gets recs that need updating" do
    expect(@si.recs_modified_after(Time.parse("2015-11-13T15:00:06.008Z").utc).count()).to eq 6135312
  end

end

