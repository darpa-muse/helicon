#! /bin/bash

# Runs doxygen over each project from the desired repository(change below) 
# Then combines all the xml doxygen output
# Then converts the combined xml to json under ./doxygen/ folder
#
#path="/data/uci2010/data/sourcerer/tera/repo"


# set default values
timeout=2700   # time to run doxygen process before bailing 
maxfiles=50000
san="false" # determine if we need to untar src code from san
count=0
countdone=0
one="false"

while [[ $# > 1 ]]
do
key="$1"

# get cmd line args
case $key in
    -max)
    maxfiles="$2"
#    echo "max files in project to process: " $maxfiles
   shift # past argument
   shift
    ;;
    -san)
    san="true"
   shift # past argument
    ;;
    -timeout)
    timeout="$2"
#   echo "timeout set: " $timout
    shift # past argument
    shift # past value
    ;;
    -one)
    one="true"
    shift
    ;;
    *)
            # unknown option
    ;;
esac
done

if [[ $# != 1 ]]; then
    echo "$0: A path to the corpus you wish to run extractor on is required."
    exit 4
fi
path=$1

#if [[ $path == *"corpus_0to7"* ]] || [[ $path == *"corpus8tof"* ]] || [[ $path == *"testSAN"* ]]; then
#  san="true"
#fi

echo "Running extractor over: $path"
echo "   max files to process: $maxfiles"
echo "   timeout set to: $timeout"
echo "   san volume detected: $san"
echo ""

# temp folder to extract archives into if using SAN
if [ $one == "false" ]; then
  tmp="home/muse/extractors/doxygen_extractor/tmp"
else
  if [[ $path == *"0to7"* ]]; then
    tmp="/home/muse/extractors/tmp07/"
  elif [[ $path == *"8tof"* ]]; then
    tmp="/home/muse/extractors/tmp8f/"
  else
    tmp="/home/muse/extractors/tmp/"
  fi
fi
mkdir -p $tmp

# Define a timestamp function
timestamp() {
  date +"%s"
}

time=$(timestamp)
numfiles=0


if [ $one == "true" ]; then
   src="echo $path"
else
   #Loop through all projects
   src="find $path -mindepth 9 -maxdepth 9 -type d"
fi

#for project in $(find $path -maxdepth 9 -mindepth 9 -type d )
#find $path -mindepth 9 -maxdepth 9 -type d  |
$src |
while read project
do
  echo "Working doxygen on $((++count)) Project: " $project 
  echo ""

  if [ ! -f $project/doxygen/doxygen.json ]
  then
    exists=false
    if [ -f $project/index.json ]; then
       code=$( cat $project/index.json | jq -r .code )
       uid=$( cat $project/index.json | jq -r .uuid )
       archive=$uid"_code.tgz"
       src_path=$project/$code

       # check to see if we need to extract a tar if on SAN
       if [ $san == "true" ]; then
          if [[ ! -d $tmp$code ]] || [[ $one == "false" ]];then
            echo "   tar xzf $project/$archive -C $tmp"
            tar xzf $project/$archive -C $tmp
          fi
          src_path=$tmp/$code
       fi

       if [ -d $src_path ] 
       then
           echo "  creating doxygen config file for project..."

           numfiles=$(ls -lR $src_path | wc -l)
      
           if [ $maxfiles ]; then
               if [[ "$numfiles" -gt "$maxfiles" ]]; then
                  echo "  project too large, skipping..."
                  continue;
               fi
           fi
           if [ $numfiles -gt 10000 ]
           then
      	       /home/muse/extractors/doxygen_extractor/make_doxyfile /home/muse/extractors/doxygen_extractor/doxyfile_skeleton_big $src_path $project/doxygen > /home/muse/extractors/doxygen_extractor/doxyfile
           else
      	       /home/muse/extractors/doxygen_extractor/make_doxyfile /home/muse/extractors/doxygen_extractor/doxyfile_skeleton $src_path $project/doxygen > /home/muse/extractors/doxygen_extractor/doxyfile
           fi 
           exists=true 
       fi

       # if doxyconfig was created (/latest or /content folder exists)
       if [ "$exists" = true  ] 
       then
          echo ""
          echo "  Running doxygen over project..."
          timeout $timeout doxygen /home/muse/extractors/doxygen_extractor/doxyfile
          echo "completed $((++countdone)) of $((count))"
          chmod a+w $project/doxygen/
          chmod a+w $project/doxygen/xml/

          echo ""
          echo "  Combining doxygen results..."
          timeout $timeout xsltproc $project/doxygen/xml/combine.xslt $project/doxygen/xml/index.xml > $project/doxygen/xml/all.xml
          echo "completed $((countdone)) of $((count))"
  
          echo ""
          echo "  Converting doxygen results to JSON..."
          timeout $timeout xml2json -t xml2json -o $project/doxygen/doxygen.json $project/doxygen/xml/all.xml --strip_text
          echo "completed $((countdone)) of $((count))"
          echo ""
       
       fi

       # tells redis this project's metadata has been updated
       redis-cli -n 0 SADD "set:metadata-updated" "$uid"

       # check to see if we need to cleanup tmp folder if using SAN
       if [ $san == "true" ] && [ $one == "false" ]; then
          rm -fr $tmp/*
       fi
    fi
  fi
     
  if [ -d $project/doxygen/xml ]
  then
      echo "  doxygen xml folder exists, removing..."
      rm -fr $project/doxygen/xml/
  fi
  echo ""
done
