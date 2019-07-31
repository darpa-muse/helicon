#! /bin/bash

#path="/data/uci2010/data/sourcerer/tera/repo"
path="/data/corpus_0to7/hackathon2"
repo=github
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
#for project in $(find $path -maxdepth 3 -mindepth 3 -type d -name $repo)
do
   project=$(echo $project | rev | cut -d "/" -f 2- | rev)
   echo ""
   echo "Working on $((count++)) Project: " $project 
   echo ""

 if [ ! -d $project/doxygen ]
 then
   exists=false
   if [ -d $project/latest ] 
   then
      numfiles=$(ls -lR $project/latest | wc -l)
      if [ $numfiles -lt 15000 ]
      then
        echo "Creating Doxygen config file for project..."
      	./make_doxyfile doxyfile_skeleton $project/latest $project/doxygen > doxyfile
        exists=true
      fi 
   fi
  
   if [ -d $project/content ] 
   then
      numfiles=$(ls -lR $project/content | wc -l)
      if [ $numfiles -lt 7500 ]
      then
        echo "Creating Doxygen config file for project..."
      	./make_doxyfile doxyfile_skeleton $project/content $project/doxygen > doxyfile
        exists=true
      fi 
   fi   

   if [ "$exists" = true  ] 
   then

     echo ""
     echo "Running doxygen over project..."
     timeout 1800 doxygen doxyfile
     echo "completed $((count))"
     chmod a+w $project/doxygen/
     chmod a+w $project/doxygen/xml/

     echo ""
     echo "Combining doxygen results..."
     timeout 1800 xsltproc $project/doxygen/xml/combine.xslt $project/doxygen/xml/index.xml > $project/doxygen/xml/all.xml
     echo "completed."
  
     echo ""
     echo "Converting Doxygen results to JSON..."
     timeout 1800 xml2json -t xml2json -o $project/doxygen/doxygen.json $project/doxygen/xml/all.xml --strip_text
     echo "completed."

#   sed -i 's/\"doxygen\":/\"timestamp\":'$time',\"version\":1,\"doxygen\":/' $project/doxygen/doxygen.json
   else
      echo "-->project is too large, skipping for now."
   fi
 else
   echo "-->doxygen folder exists, skipping project"
   rm -fr $project/doxygen/xml/
 fi
 echo ""
done
