#!/usr/bin/bash

exp=$1

#### Experiment 0 - S3Fs Caching vs No Caching vs Prefetching
# echo "Executing experiment 0"

#### Experiment 1 -  Block Size

if [[ $exp == *"1"* ]]
then
	echo "Executing experiment 1"

	python nib_read.py --block_size 8388608 --types mem --types prefetch --types s3fs --lazy --reps 10
	python nib_read.py --block_size 16777216 --types mem --types prefetch --types s3fs --lazy --reps 10
	python nib_read.py --block_size 33554432 --types mem --types prefetch --types s3fs --lazy --reps 10
	python nib_read.py --block_size 67108864 --types mem --types prefetch --types s3fs --lazy --reps 10
	python nib_read.py --block_size 134217728 --types mem --types prefetch --types s3fs --lazy --reps 10
	python nib_read.py --block_size 268435456 --types mem --types prefetch --types s3fs --lazy --reps 10
	python nib_read.py --block_size 536870912 --types mem --types prefetch --types s3fs --lazy --reps 10
	python nib_read.py --block_size 1073741824 --types mem --types prefetch --types s3fs --lazy --reps 10
	python nib_read.py --block_size 2147483648 --types mem --types prefetch --types s3fs --lazy --reps 10
	mv ../results/us-west-2-xlarge/readnib* ../results/us-west-2-xlarge/exp-1
fi

if [[ $exp == *"2"* ]]
then

	#### Experiment 2 - Number of files
	echo "Executing experiment 2"

	python nib_read.py --n_files 1 --types mem --types prefetch --types s3fs --lazy --reps 10
	python nib_read.py --n_files 5 --types mem --types prefetch --types s3fs --lazy --reps 10
	python nib_read.py --n_files 10 --types prefetch --types s3fs --lazy --reps 10
	python nib_read.py --n_files 15 --types prefetch --types s3fs --lazy --reps 10
	python nib_read.py --n_files 20 --types prefetch --types s3fs --lazy --reps 10
	python nib_read.py --n_files 25 --types prefetch --types s3fs --lazy --reps 10

	mv ../results/us-west-2-xlarge/readnib* ../results/us-west-2-xlarge/exp-2

fi

if [[ $exp == *"3"* ]]
then

	#### Experiment 3 - Nibabel vs Python read comparison
	echo "Executing experiment 3"
	python cmp_read.py

fi

if [[ $exp == *"4"* ]]
then

	#### Experiment 4 - Parallel
	echo "Executing experiment 4"

	fs=( "s3fs" "prefetch" )
	for i in 1,5,10,15,20,25
	do
		for j in {0..10}
		do
		    fs=( $(shuf -e "${fs[@]}") )
		    for f in "${fs[@]}"
		    do
			if [ "$f" = "s3fs" ]
			then
			    python read_s3fs.py 0 $i $j 4 &
			    python read_s3fs.py $i $(( i*2 )) $j 4 &
			    python read_s3fs.py $(( i*2 )) $(( i*3 )) $j 4 &
			    python read_s3fs.py $(( i*3 )) $(( i*4 )) $j 4 &
			else
			    python read_prefetched.py 0 $i $j 4 &
			    python read_prefetched.py $i $(( i*2 )) $j 4 &
			    python read_prefetched.py $(( i*2 )) $(( i*3 )) $j 4 &
			    python read_prefetched.py $(( i*3 )) $(( i*4 )) $j 4 &
			fi

			wait < <(jobs -p)

		    done
		done
	done
	mv ../results/us-west-2-xlarge/read_prefetch* ../results/us-west-2-xlarge/exp-4
	mv ../results/us-west-2-xlarge/read_s3fs* ../results/us-west-2-xlarge/exp-4
fi
