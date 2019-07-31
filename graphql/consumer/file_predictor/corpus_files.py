#!/usr/bin/env python

import sys
import re
import os
import time
import subprocess


main_file = open(sys.argv[1])
project_uuid = sys.argv[2]
output_file = open(sys.argv[3], "a")





#holder = "/mnt/ssd/rc_files/holder/"

holder = os.environ["PROJECT_LOCATION"]
output_location = os.environ["FILE_OUTPUT_LOCATION"]


main_file_lines = main_file.readlines()


counter =0



extmap = [".asm", ".c", ".c++", ".cc", ".cpp", ".cs", ".cxx", ".h", ".hh", ".hxx", ".java", ".js", ".m", ".mm", ".php", ".pl", ".py", ".r", ".rb", ".scala", ".sh", ".vb"]





for line in main_file_lines:

        uuid = main_file_lines[counter].split('  ')[0]
        print uuid
        first_four = uuid[0:4]
        print first_four
        file = main_file_lines[counter].split('  ')[1]
        print file
        #get file extension
        file_ext = str("."+file.split('/')[-1].split(".")[-1]).strip()
        print file_ext

        #get file path
        #going to have to play with this a bit...
        file_parts = file.split("/")[5:]
        print file_parts
        file_string =""
        for part in file_parts:
        	file_string = file_string + part + "/"

        file_string = file_string[:-1].strip()
        print file_string
        #check if file ext in array
        if file_ext in extmap:

          #score we care about this file save it
          #cp uuid to output_location/first_four/ 
          print "we have a match: "+ file_ext
          #mkdir
          os.system('mkdir -p '+output_location+first_four)

          #copy file
          command = "cp "+holder+"/"+file_string+"  "+output_location+first_four+"/"+uuid
          print command
          os.system(command)
          #write to file uuid, hash 
          new_line = project_uuid+","+uuid+"\n"
          print new_line
          output_file.write(new_line)
          #os.system(command)
          


        
        counter = counter + 1

