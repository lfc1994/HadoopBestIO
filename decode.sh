#!/bin/bash
mv hdfs/data/current/NS-244702620/current/finalized/blk_6308292913955508887* ./
bin/stop-all.sh
./start-cluster.sh
./deploy_raidnode.sh
bin/hadoop raidshell -recoverBlocks /user/JackJay/ps.dmg.zip
echo "origin block sha1:"
sha1sum ./blk_6308292913955508887
echo "recovered block sha1:"
sha1sum hdfs/data/current/NS-244702620/current/finalized/blk_6308292913955508887
