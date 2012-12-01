#!/bin/bash
#Usage ./step2.sh serverdb pass user port

sdb="$1:$4/cc"

java -jar StorePR.jar $sdb $2 $3

