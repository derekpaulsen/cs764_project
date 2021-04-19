#!/usr/bin/fish

set -l num_reps 5

set -l f 'seq_insert.txt'
echo "running $f"
for i in (seq 32)
	echo -n "num threads $i"
	set -x OMP_NUM_THREADS $i

	for j in (seq $num_reps)
		echo -n "."
		timeout 5m ./vanilla ./workload/$f 1>> res.json
		if test $status != 0
			echo 'TIMED OUT'
		end

	end
	echo "done"
end



set -l f 'seq_insert_with_hc_read.txt'
echo "running $f"
for i in (seq 32)
	echo -n "num threads $i"
	set -x OMP_NUM_THREADS $i

	for j in (seq $num_reps)
		echo -n "."
		timeout 5m ./vanilla ./workload/$f 1>> res.json
		if test $status != 0
			echo 'TIMED OUT'
		end

	end
	echo "done"
end
