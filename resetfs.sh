#!/bin/sh
echo "stopping dfs:"
bin/stop-dfs.sh

echo "removing dfs local storage:"
rm hdfs/data hdfs/name -r

echo "format dfs:"
bin/hadoop namenode -format

echo "starting dfs:"
./start-cluster.sh

echo "put ps.dmg into dfs:"
bin/hadoop fs -put ../ps.dmg ps.dmg

echo "stopping dfs:"
bin/stop-dfs.sh
