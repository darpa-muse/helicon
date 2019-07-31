#! /bin/bash

repo=maven
count=1
# Define a timestamp function
timestamp() {
  date +"%s"
}

time=$(timestamp)

# Lets iterate through the uuid directories
#PROJECTS= find /data/test/ -maxdepth 9 -mindepth 9 -type d
for projects in $(find /data/maven/maven -type d -name latest)
do
   project=$(echo $projects | rev | cut -d "/" -f 2- | rev)
   echo ""
   echo "Working on $((count++)) Project: " $project 
   echo ""

 if [ ! -d $project/doxygen ]
 then
   exists=false
   echo "Creating Doxygen config file for project..."
   if [ -d $project/latest ] 
   then
      ./make_doxyfile doxyfile_skeleton $project/latest $project/doxygen > doxyfile
      exists=true 
   fi
  
   if [ -d $project/content ] 
   then
      ./make_doxyfile doxyfile_skeleton $project/content $project/doxygen > doxyfile
      exists=true 
   fi   

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
   
   fi
 else
   echo "doxygen folder exists, skipping project"
   rm -fr $project/doxygen/xml
   echo ""
 fi
done
