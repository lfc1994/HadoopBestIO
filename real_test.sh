#!/bin/bash

namespaceId="1016582808"
filename="ps.dmg"
blkId="582370057409509901"
code="best_io"

echo "stop cluster:"
bin/stop-all.sh > /dev/null

echo "move back block file:"
mv ./blk_* hdfs/data/current/NS-${namespaceId}/current/finalized/


echo "deploy raidnode:"
./deploy_raidnode.sh

echo "starting cluster:"
./start-cluster.sh

echo "remove parity files:"
bin/hadoop fs -rmr /raid_*
bin/hadoop fs -rmr .Trash

echo "raid file with ${code} code"
echo "filename is ${filename}"
bin/hadoop raidshell -raidFile /user/JackJay/${filename} /raid_${code} ${code} 1>log.txt 2>>log.txt

echo "sha1 specified block and remove it"
echo "NS ID is ${namespaceId}:"
echo "test blk Id is ${blkId}:"
sha1sum hdfs/data/current/NS-${namespaceId}/current/finalized/blk_${blkId} > sha1.txt
mv hdfs/data/current/NS-${namespaceId}/current/finalized/blk_${blkId}* ./

echo "restart cluster:"
bin/stop-all.sh > /dev/null
./start-cluster.sh

echo "recover Blocks:"
echo -e "\nline\n" >> log.txt
bin/hadoop raidshell -recoverBlocks /user/JackJay/${filename} 1 >> log.txt 2>>log.txt

echo "show hex diff:"
sha1sum hdfs/data/current/NS-${namespaceId}/current/finalized/blk_${blkId} >> sha1.txt

echo "show sha1 diff:"
cat sha1.txt

