#!/bin/bash
cd src/contrib/raid
ant | grep 'BUILD'
cd ~/workspace/HadoopUSC
mv build/contrib/raid/hadoop-codec-1.2-raid.jar lib/
