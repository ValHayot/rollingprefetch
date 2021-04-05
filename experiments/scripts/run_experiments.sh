#!/usr/bin/bash

exp=$1

#### Experiment 0 - S3Fs Caching vs No Caching vs Prefetching
# echo "Executing experiment 0"

#### Experiment 1 -  Block Size

if [[ $exp == *"1"* ]]
then
	echo "Executing experiment 1"

	python nib_read.py --block_size 8388608 --types mem --types prefetch --types s3fs --lazy
	python nib_read.py --block_size 16777216 --types mem --types prefetch --types s3fs --lazy
	python nib_read.py --block_size 33554432 --types mem --types prefetch --types s3fs --lazy
	python nib_read.py --block_size 67108864 --types mem --types prefetch --types s3fs --lazy
	python nib_read.py --block_size 134217728 --types mem --types prefetch --types s3fs --lazy

fi

if [[ $exp == *"2"* ]]
then

	#### Experiment 2 - Number of files
	echo "Executing experiment 2"

	python nib_read.py --n_files 1 --types prefetch --types s3fs --lazy
	python nib_read.py --n_files 5 --types prefetch --types s3fs --lazy
	python nib_read.py --n_files 10 --types prefetch --types s3fs --lazy
	python nib_read.py --n_files 15 --types prefetch --types s3fs --lazy
	python nib_read.py --n_files 20 --types prefetch --types s3fs --lazy

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
	for i in {0..5}
	do
	    fs=( $(shuf -e "${fs[@]}") )
	    for f in "${fs[@]}"
	    do
		if [ "$f" = "s3fs" ]
		then
		    python read_s3fs.py 0 20 $i 4 &
		    python read_s3fs.py 20 40 $i 4 &
		    python read_s3fs.py 40 60 $i 4 &
		    python read_s3fs.py 60 80 $i 4 &
		else
		    python read_prefetched.py 0 20 $i 4 &
		    python read_prefetched.py 20 40 $i 4 &
		    python read_prefetched.py 40 60 $i 4 &
		    python read_prefetched.py 60 80 $i 4 &
		fi

		wait < <(jobs -p)

	    done
	done
fi
