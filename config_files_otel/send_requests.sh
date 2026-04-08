#!/usr/bin/env bash

#URL="http://127.0.0.1/work?fail=0.8&ms=100"
URL="http://127.0.0.1/process_transaction"

while true; do
    curl -4 'http://localhost/process_transaction?total=100'
    sleep 1
done