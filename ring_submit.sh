#!/bin/bash
for i in {2,4,8,16,32}
do
    for j in {1..5}
    do
    MEM=$((64/i))
    sbatch   --mem-per-cpu=${MEM}G --constraint=elves  --ntasks-per-node=1 --nodes=$i --time="03:00:00"  ring.sh $i
    done
done