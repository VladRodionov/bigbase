#!/usr/bin/env bash
#
# Row-Cache Coprocessor administration tool. It allows to enable/disable 
# row caching per table and per table:column family 
# 

usage="Usage: rcadmin.sh \
 (status|enable|disable|list) <hbase-table-name> \
 <column-family-name> (optional)"

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

$JAVA_HOME/bin/java -cp .:../*:../lib/*:../conf com.inclouds.hbase.utils.ConfigManager $1 $2 $3


