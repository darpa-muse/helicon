#! /bin/bash

path="/data/corpus/"
count=1
# Define a timestamp function
timestamp() {
  date +"%s"
}

time=$(timestamp)
numfiles=0

# Lets iterate through the uuid directories
#PROJECTS= find /data/test/ -maxdepth 9 -mindepth 9 -type d
for project in $(find $path -maxdepth 9 -mindepth 9 -type d )
do
   project=$(echo $project)
   echo ""
   echo "Working on $((count++)) Project: " $project 
   echo ""

 if [ -d $project/doxygen ]
 then
   echo "doxygen folder exists, cleaning project"
   rm -fr $project/doxygen/xml/
   echo ""
 fi
done
