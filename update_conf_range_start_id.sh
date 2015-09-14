#!/bin/bash

start_id=$(python get_last_id.py)
sed -i "s/start_id: [0-9]*/start_id: $start_id/" conf.ini 
rm -f sucess_id.log
rm -f fail_id.log
rm -f log.txt
rm -rf backup
tail -1 result.txt > result.tmp; mv result.tmp result.txt
mkdir backup


