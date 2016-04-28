#!/bin/bash
bin/start-dfs.sh
bin/hadoop dfsadmin -safemode leave
echo "wait"
read x
