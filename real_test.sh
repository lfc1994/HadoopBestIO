#!/bin/bash

namespaceId="128487345"
hdfsHome="/Users/JackJay/workspace/HadoopUSC/hdfs"
hadoopHome="/Users/JackJay/workspace/HadoopUSC"
filename="/user/JackJay/ps.dmg"
blkId="4208763921562130464"
code="lrc2"

echo "stop cluster:"
bin/stop-all.sh > /dev/null

echo "move back block file:"
mv ${hadoopHome}/blk_* ${hdfsHome}/data/current/NS-${namespaceId}/current/finalized/


echo "deploy raidnode:"
./deploy_raidnode.sh

echo "starting cluster:"
./start-cluster.sh

echo "remove parity files:"
bin/hadoop fs -rmr /raid_*
bin/hadoop fs -rmr .Trash

echo "raid file with ${code} code"
echo "filename is ${filename}"
bin/hadoop raidshell -raidFile ${filename} /raid_${code} ${code} 1>log.txt 2>>log.txt

echo "sha1 specified block and remove it"
echo "NS ID is ${namespaceId}:"
echo "test blk Id is ${blkId}:"
sha1sum ${hdfsHome}/data/current/NS-${namespaceId}/current/finalized/blk_${blkId} > sha1.txt
mv ${hdfsHome}/data/current/NS-${namespaceId}/current/finalized/blk_${blkId}* ${hadoopHome}

echo "restart cluster:"
bin/stop-all.sh > /dev/null
./start-cluster.sh

echo "recover Blocks:"
echo -e "\nline\n" >> log.txt
bin/hadoop raidshell -recoverBlocks ${filename} 1 >> log.txt 2>>log.txt

echo "show hex diff:"
sha1sum ${hdfsHome}/data/current/NS-${namespaceId}/current/finalized/blk_${blkId} >> sha1.txt

echo "show sha1 diff:"
cat sha1.txt

