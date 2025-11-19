#!/bin/sh
set -eu
ES_BASE="http://elastic:9200"

#curl -fsS -X PUT "$ES_BASE/tijdschriften-plaatsnaam" -H "Content-Type: application/json" --data @/seed/index-plaatsnaam.json
#curl -fsS -X POST "$ES_BASE/tijdschriften-plaatsnaam/_bulk" -H "Content-Type: application/x-ndjson" --data-binary @/seed/bulk-plaatsnaam.ndjson
#curl -fsS -X PUT "$ES_BASE/tijdschriften-personen" -H "Content-Type: application/json" --data @/seed/index-personen.json
#curl -fsS -X POST "$ES_BASE/tijdschriften-personen/_bulk" -H "Content-Type: application/x-ndjson" --data-binary @/seed/bulk-personen.ndjson
#curl -fsS -X PUT "$ES_BASE/tijdschriften" -H "Content-Type: application/json" --data @/seed/index-tijdschriften.json
#curl -fsS -X POST "$ES_BASE/tijdschriften/_bulk" -H "Content-Type: application/x-ndjson" --data-binary @/seed/bulk-tijdschriften.ndjson
#curl -fsS -X PUT "$ES_BASE/tijdschriften-uitgever_drukker" -H "Content-Type: application/json" --data @/seed/index-uitgever_drukker.json
#curl -fsS -X POST "$ES_BASE/tijdschriften-uitgever_drukker/_bulk" -H "Content-Type: application/x-ndjson" --data-binary @/seed/bulk-uitgever_drukker.ndjson

curl -fsS -X PUT "$ES_BASE/hi-ga-politieke-tijdschriften" -H "Content-Type: application/json" --data @/seed/index-tijdschriften.json
curl -fsS -X POST "$ES_BASE/hi-ga-politieke-tijdschriften/_bulk" -H "Content-Type: application/x-ndjson" --data-binary @/seed/bulk-tijdschriften.ndjson
