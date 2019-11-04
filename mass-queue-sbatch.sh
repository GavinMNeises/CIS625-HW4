declare -A times
times[2]="2:30:00"
times[4]="1:30:00"
times[8]="1:00:00"
times[16]="0:45:00"
times[20]="0:30:00"

for x in 2 4 8 16
do
	MEM=$((64/x))
	sbatch --mem-per-cpu=${MEM}G --constraint=elves --ntasks-per-node=1 --nodes=$x --time=${times[$x]} queue-sbatch.sh $x
done;