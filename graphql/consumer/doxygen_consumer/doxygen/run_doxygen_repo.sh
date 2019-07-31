#! /bin/bash

# Runs doxygen over each project from the desired repository(change below) 
# Then combines all the xml doxygen output
# Then converts the combined xml to json under ./doxygen/ folder
#
path="/data/corpus/"
repo=uciMaven
count=1
# Define a timestamp function
timestamp() {
  date +"%s"
}

time=$(timestamp)
numfiles=0

# Lets iterate through the uuid directories
#PROJECTS= find /data/test/ -maxdepth 9 -mindepth 9 -type d
for project in $(find $path -maxdepth 10 -mindepth 10 -type d -name $repo)
do
   project=$(echo $project | rev | cut -d "/" -f 2- | rev)
   echo ""
   echo "Working on $((count++)) Project: " $project 
   echo ""

   code=$(jq -r .code $project/index.json)

  if [ ! -d $project/doxygen ]
  then
   exists=false
   if [ -d $project/$code ] 
   then
      echo "Creating Doxygen config file for project..."
      numfiles=$(ls -lR $project/$code | wc -l)
      if [ $numfiles -lt 10000 ]
      then
      	./make_doxyfile doxyfile_skeleton $project/$code $project/doxygen > doxyfile
      else
      	./make_doxyfile doxyfile_skeleton_big $project/$code $project/doxygen > doxyfile
      fi 
      exists=true 
   else 
      echo "No source folder exists, skipping..."
   fi

   # if doxyconfig was created (/latest or /content folder exists)
   if [ "$exists" = true  ] 
   then

     echo ""
     echo "Running doxygen over project..."
     doxygen doxyfile
     echo "completed $((count))"
     chmod a+w $project/doxygen/
     chmod a+w $project/doxygen/xml/

     echo ""
     echo "Combining doxygen results..."
     xsltproc $project/doxygen/xml/combine.xslt $project/doxygen/xml/index.xml > $project/doxygen/xml/all.xml
     echo "completed."
  
     echo ""
     echo "Converting Doxygen results to JSON..."
     xml2json -t xml2json -o $project/doxygen/doxygen.json $project/doxygen/xml/all.xml --strip_text
     echo "completed."
     echo ""

#   sed -i 's/\"doxygen\":/\"timestamp\":'$time',\"version\":1,\"doxygen\":/' $project/doxygen/doxygen.json
     
     #if doxygen.json was created, remove intermidiate /xml folder
     if [ -f $project/doxygen/doxygen.json ]
     then
       rm -fr $project/doxygen/xml
     fi

   fi
  else
   echo "doxygen folder exists, skipping project"
   if [ -d $project/doxygen/xml ]
   then
     echo "doxygen xml folder exists, removing..."
     rm -fr $project/doxygen/xml/
   fi
   echo ""
  fi
done
