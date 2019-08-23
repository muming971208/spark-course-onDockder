#!/bin/bash

local_file_path=$1

hadoop_hdfs_path=$2

hadoop dfs -mkdir -p /$USER/bigdata

if hadoop fs -test -e $2 then echo "The file already exists";
else hadoop dfs -put $1 $2
fi