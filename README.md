> [!IMPORTANT]  
> As of January 2026, the registry and supporting code for the HathiTrust federal documents repository is defunct. This code is archived and is not maintained.

```ruby
require 'dotenv'
require_relative 'lib/solrindex'
Dotenv.load

si = SolrIndex.new( ENV['solr_host'], 9035 )
num = si.update
puts "updated #{num}"
```
