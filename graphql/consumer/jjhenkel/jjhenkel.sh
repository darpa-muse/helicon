#!/bin/bash

id=$1

curr_loc=$(pwd)
cd /projects/$id

cd ./*/
pwd

#make
timeout 300 make -j$(nproc)

# Check status
OUT=$?
if [ $OUT -eq 0 ];then
   echo "Build (make) succeeded."
   #echo "$uid,success" >> /mnt/output.csv
   python $curr_loc/update_build.py $id success
else
   echo "Build failed."
   #echo "$uid,failed" >> /mnt/output.csv
   python $curr_loc/update_build.py $id failed
fi
