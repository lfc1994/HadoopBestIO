#!/bin/bash
bin/start-dfs.sh
for i in $(seq 1 5)
do
	echo $i
	sleep 1
done
bin/hadoop dfsadmin -safemode leave
bin/start-mapred.sh
bin/start-raidnode.sh
