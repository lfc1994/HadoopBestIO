#!/bin/bash
bin/stop-raidnode.sh
cd src/contrib/raid
ant | grep 'BUILD'
cd ~/workspace/HadoopUSC
mv build/contrib/raid/hadoop-codec-1.2-raid.jar lib/
bin/start-raidnode.sh
jps | grep RaidNode
