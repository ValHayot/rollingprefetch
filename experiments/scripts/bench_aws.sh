#!/usr/bin/bash

output="../results/filetransfer.bench"
echo "fs,repetition,size,real,user,system" > $output

bench_aws () {
	
	mb=$1
	repetition=$2
	fs="aws"

	# clear caches
	echo 3 | sudo tee /proc/sys/vm/drop_caches

	#read file and store benchmark in variable
	runtime=$(/usr/bin/time -f %e,%U,%S aws s3 cp s3://vhs-bucket/rand${mb}.out - 2>&1 > /dev/null)
	echo "${fs},${r},${mb},${runtime}" >> $output
}

bench_local() {
	mb=$1
	repetition=$2
	fs=$3

	file="/dev/shm/rand${mb}.out"
	echo "file $file"

	if [ "local" = $fs ]
	then
		file="/home/ec2-user/rand${mb}.out"
	fi

	# get file from aws
	aws s3 cp s3://vhs-bucket/rand${mb}.out $file

	# clear caches
	echo 3 | sudo tee /proc/sys/vm/drop_caches

	#read file and store benchmark in variable
	runtime=$(/usr/bin/time -f %e,%U,%S dd if=$file of=/dev/null bs=5M 2>&1 > /dev/null )
	runtime=${runtime##*$'\n'}
	echo "${fs},${r},${mb},${runtime}" >> $output

	# cleanup
	rm $file

}

filesystems=( "aws" "local" "mem" )
reps=$1

for r in $( seq 0 $reps )
do
	for i in {1..11}
	do
		filesystems=( $(shuf -e "${filesystems[@]}") )
		
		for fs in "${filesystems[@]}"
		do
			# get number of bytes to read
			mb=$((2**i))

			if [ "aws" == $fs ]
			then
				echo "executing aws $r $fs $mb"
				bench_aws $mb $r
			else
				echo "executing $fs $r $mb"
				bench_local $mb $r $fs

			fi

		done
	done
done



