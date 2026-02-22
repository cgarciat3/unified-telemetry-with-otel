#!/usr/bin/env bash

#URL="http://127.0.0.1/work?fail=0.8&ms=100"
URL="http://127.0.0.1/process_transaction"

while true; do
    #curl -s -o /dev/null -w "%{http_code}\n" "$URL"
    curl -X POST "http://localhost/process_transaction" -H "Content-Type: application/json" -d '{"amount": 50, "currency": "EUR"}'
    sleep 0.3
done
