#!/bin/bash
bin/stop-all.sh
./deploy_raidnode.sh
./start-cluster.sh
bin/hadoop raidshell -test /user/JackJay/ps.dmg /raid_lrc1/user/JackJay/ps.dmg lrc1
