#!/bin/bash
blk0="-6789833647027949669"
blk1="-1740410296281517994"
blk2="-644316217557088614"
blk3="731312282387620320"
blk4="-4871751929244142138"
path="/Users/JackJay/workspace/HadoopUSC/hdfs/data/current/NS-1016582808/current/finalized"
offset=8388608

echo "src and p1 data:"
xxd -s ${offset} -l 8 ${path}/blk_${blk0}
xxd -s ${offset} -l 8 ${path}/blk_${blk1}
xxd -s ${offset} -l 8 ${path}/blk_${blk2}
xxd -s ${offset} -l 8 ${path}/blk_${blk3}
xxd -s ${offset} -l 8 ${path}/blk_${blk4}

echo "original"
xxd -s ${offset} -l 8 ./blk_${blk1}