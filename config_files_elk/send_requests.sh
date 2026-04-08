#!/usr/bin/env bash

#URL="http://127.0.0.1/work?fail=0.8&ms=100"
URL="http://127.0.0.1/process_transaction?total=100"

    #curl -s -o /dev/null -w "%{http_code}\n" "$URL"
    #curl "http://127.0.0.1/maintenance"
    #sleep 1



while true; do
    curl "http://127.0.0.1/process_transaction?total=100&fail_rate=0.3&mult=5"
    sleep 0.5
done