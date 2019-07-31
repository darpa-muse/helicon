#!/bin/bash

id=$1
loc=$2

curr_loc=$(pwd)
cd $loc
repo=$(ls | awk '{ print $1 }')


output=$loc/md5deep.txt


#timeout 2700 md5deep -r $loc/$repo > $output
md5deep -r $loc/$repo > $output


python $curr_loc/corpus_files.py $output $id $curr_loc/output.csv
