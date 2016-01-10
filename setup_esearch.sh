# run to initialize elastic search with timestamps until we can
# just get the cyanite code to do this by default
curl -XPOST localhost:9200/cyanite_paths -d '{
"settings" : {
    "number_of_shards" : 3,
    number_of_replicas : 1
},
"mappings" : {
    "path": {
        "_timestamp" : {
            "enabled" : true,
            "store" : true
        },
        "_ttl" : {
            "enabled" : true,
            "default" : "1d",
        },
    "properties":{
            "depth":{
            "type":"long"
            },
            "leaf":{
            "type":"boolean"
            },
            "path":{
            "type":"string","index":"not_analyzed"
            },
            "tenant":{
            "type":"string","index":"not_analyzed"
            }
        }
    }
  }
}'
