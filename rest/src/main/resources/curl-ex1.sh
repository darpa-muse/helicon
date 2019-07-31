#!/bin/bash 

# A curl example of search
q=$(echo -n "language=java and language = c" | base64)
out=$(curl -s -H "Accept: application/json" "http://docker-accumulo:8052/muse/search?q="$q)
echo $out | jq .

: <<'end_long_comment'
android=$(echo -n "android" | base64)
out=$(curl -sS -H "Accept: application/json" "http://docker-accumulo:8052/muse/categories/topics/"$android)
echo $out | jq .

end_long_comment