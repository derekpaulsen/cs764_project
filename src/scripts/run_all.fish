#!/usr/bin/fish

set -l num_reps 5
set -l 

for f in (ls ./workload)
	echo "running $f"
	for i in (seq 32)
		echo -n "num threads $i"
		set -x OMP_NUM_THREADS $i

		for j in (seq $num_reps)
			echo -n "."
			#./vanilla ./workload/$f
		end
		echo "done"
	end
end

