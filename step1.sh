#!/bin/bash
#Usage ./step1.sh serverdb pass user port

mysql -h $1 -u $3 -p$2 < cc.sql

sdb="$1:$4/cc"

java -jar WebCrawlerIndexer.jar $sdb $2 $3

rm graph.txt
