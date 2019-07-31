#! /bin/bash

path="/data/corpus/"
repo=github
count=1
# Define a timestamp function
timestamp() {
  date +"%s"
}
project="/data/corpus/1/5/f/e/5/3/e/c/15fe53ec-cd9d-11e4-8a8b-ef737ae8825b"
time=$(timestamp)
numfiles=0

   echo "Working on Project: " $project 
   echo ""

 if [ ! -f $project/doxygen/doxygen.json ]
 then
   exists=false
   echo "Creating Doxygen config file for project..."
   if [ -d $project/latest ] 
   then
      numfiles=$(ls -lR $project/latest | wc -l)
      if [ $numfiles -gt 1000 ]
      then
      	./make_doxyfile doxyfile_skeleton_big $project/latest $project/doxygen > doxyfile
      else
      	./make_doxyfile doxyfile_skeleton $project/latest $project/doxygen > doxyfile
      fi 
      exists=true 
   fi
  
   if [ -d $project/content ] 
   then
      numfiles=$(ls -lR $project/content | wc -l)
      if [ $numfiles -gt 1000 ]
      then
      	./make_doxyfile doxyfile_skeleton_big $project/content $project/doxygen > doxyfile
      else
      	./make_doxyfile doxyfile_skeleton $project/content $project/doxygen > doxyfile
      fi 
      exists=true 
   fi   

   if [ "$exists" = true  ] 
   then

   echo ""
   echo "Running doxygen over project..."
   doxygen doxyfile
   echo "completed"
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
   
   fi
 else
   echo "doxygen folder exists, skipping project"
#  rm -fr $project/doxygen/xml/
   echo ""
 fi
