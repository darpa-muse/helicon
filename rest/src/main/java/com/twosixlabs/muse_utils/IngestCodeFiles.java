package com.twosixlabs.muse_utils;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IngestCodeFiles {
    public static void main (String[] args){
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", "hdfs://docker-accumulo:8020");
            FileSystem filesystem = FileSystem.get(configuration);

            filesystem.copyToLocalFile(new Path("/corpus/5.0/0010f14b-d6d7-41d9-8cef-7d806c942197/0010f14b-d6d7-41d9-8cef-7d806c942197_code.tgz"), new Path("/tmp"));

        }catch (IOException e) {
            e.printStackTrace();

        }
    }
}
