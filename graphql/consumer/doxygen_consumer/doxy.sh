#!/bin/bash

id=$1
loc=$2
timeout=2700   # time to run doxygen process before bailing 
maxfiles=50000

python /home/muse/start_doxygen.py $id

curr_loc=$(pwd)
cd $loc
repo=$(ls -d */ | grep -v "doxygen")

src_path=$loc/$repo

mkdir -p $loc/doxygen/
mkdir -p $loc/doxygen/xml/




numfiles=$(ls -lR $src_path | wc -l)
      

FILE=$loc/doxygen/doxygen.json
if [ -f "$FILE" ] 
  then
    exists=true
    echo "doxygen done already"
    python /home/muse/update_doxygen.py $id
    exit 1
  else
    exists=false
fi    

# if doxyconfig was created (/latest or /content folder exists)
  if [ "$exists" = false  ] 
    then

      if [ $numfiles -gt 10000 ]
        then
          /doxygen/make_doxyfile /doxygen/doxyfile_skeleton_big $src_path $loc/doxygen/ > /doxygen/doxyfile
        else
          /doxygen/make_doxyfile /doxygen/doxyfile_skeleton $src_path $loc/doxygen/ > /doxygen/doxyfile
      fi 
      echo ""
      echo "  Running doxygen over project..."
      timeout $timeout doxygen /doxygen/doxyfile
      echo "completed $((++countdone)) of $((count))"

      chmod a+w $loc/doxygen/
      chmod a+w $loc/doxygen/xml/

      echo ""
      echo "  Combining doxygen results..."
      timeout $timeout xsltproc $loc/doxygen/xml/combine.xslt $loc/doxygen/xml/index.xml > $loc/doxygen/xml/all.xml
      echo "completed $((countdone)) of $((count))"
  
      echo ""
      echo "  Converting doxygen results to JSON..."
      timeout $timeout xml2json $loc/doxygen/xml/all.xml $loc/doxygen/doxygen.json
      echo "completed $((countdone)) of $((count))"
      echo ""

      
       
  fi


python /home/muse/update_doxygen.py $id
