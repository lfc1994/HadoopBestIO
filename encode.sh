#!/bin/bash
bin/hadoop fs -rmr /raid_best_io
bin/hadoop fs -rmr .Trash
bin/hadoop raidshell -raidFile /user/JackJay/ps.dmg.zip /raid_best_io best_io
