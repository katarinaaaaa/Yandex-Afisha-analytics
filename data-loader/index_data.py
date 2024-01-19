from elasticsearch import Elasticsearch
import json
import os
import requests

MAPPING_FOR_INDEX = {
    "properties": {
        "city_id": {"type": "text", "fielddata": "true"},
        "date": {"type": "date", "format": "dd_MM_yyyy"},
        "title": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword"
                }
            },
            "fielddata": "true"
        },
        "contentRating": {"type": "text", "fielddata": "true"},
        "userRating": {
            "properties": {
                "value": {"type": "float"},
                "count": {"type": "integer"}
            }
        },
        "type": {
            "properties": {
                "name": {"type": "text", "fielddata": "true"},
                "type": {"type": "text", "fielddata": "true"}
            }
        },
        "tags": {
            "properties": {
                "name": {"type": "text", "fielddata": "true"},
                "type": {"type": "text", "fielddata": "true"}
            }
        },
        "price": {
            "properties": {
                "min": {"type": "integer"},
                "max": {"type": "integer"}
            }
        },
        "place": {
            "properties": {
                "tags": {
                    "properties": {
                        "name": {"type": "text", "fielddata": "true"}
                    }
                },
                "title": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword"
                        }
                    },
                    "fielddata": "true"
                },
                "address": {"type": "text", "fielddata": "true"},
                "city": {"type": "text", "fielddata": "true"},
                "location": {"type": "geo_point"},
                "metro": {"type": "text", "fielddata": "true"}
            }
        },
        "pushkinCardAllowed": {"type": "boolean"}
    }
}

es = Elasticsearch("http://elasticsearch:9200")

if es.indices.exists(index="events"):
    es.indices.delete(index="events")

es.indices.create(index="events", mappings=MAPPING_FOR_INDEX)

events = os.listdir('storage/data')
print("Data indexation started")
for e in events:
    with open(f"storage/data/{e}", encoding="utf-8") as f:
        doc = json.load(f)
    es.index(index="events", document=doc)
    print(e)
print("All data is loaded")

# load dashboard
files = {
    'file': open('dashboard.ndjson', 'rb'),
}
response = requests.post('http://kibana:5601/api/saved_objects/_import',
                         headers={'kbn-xsrf': 'true'}, files=files)
print()
print('To see visualisation go to http://localhost:5601 in your browser and open Dashboard "Events"')
