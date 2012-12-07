/*
 * Cloud9: A MapReduce Library for Hadoop
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.example.join;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.pig.data.Tuple;


/**
 * <p>
 * Sort Partition Sequence File demo. This Hadoop Tool reads {@link Tuple} 
 * objects from a SequenceFile, sorts and partitions them based on the 
 * SequenceFile key. The output is written as SequenceFile(s) according to 
 * the number of partitions specified at runtime. If the UNIQUE flag is set, 
 * only one tuple will be written to output for each group of tuples that share
 * a common key. 
 * This tool takes the following command-line arguments:
 * </p>
 * 
 * <ul>
 * <li>[input-path] input path</li>
 * <li>[output-path] output path</li>
 * <li>[unique=?] set unique=1 for unique mode, else set unique=0</li>
 * <li>[key-class] Class of the SequenceFile key field</li>
 * <li>[value-class] Class of the SequenceFile value field</li>
 * <li>[num-partitions] number of partitions</li>
 * </ul>
 * 
 * <p>
 * The output of this tool can be used as input to {@link DemoMapSideJoin}.
 * </p>
 * 
 * @see DemoPackTuples
 * @see DemoMapSideJoin
 * 
 * @author Mohamed M. Abdul Rasheed
 */


public class SortPartitionSequenceFile extends Configured implements Tool {
  
  public static final Logger sLogger = Logger.getLogger(SortPartitionSequenceFile.class);
  
  public static class MapClass extends Mapper<Object, Object, Object, Object> {
    
    @Override
    public void map(Object key, Object value, Context context) 
        throws IOException, InterruptedException {
      context.write(key, value);
    }
  }
  
  public static class ReduceClass extends Reducer<Object, Object, Object, Object> {
    
    private static Configuration jobConf;
    private static boolean unique = false;
    
    @Override
    public void setup(Context context) {
      jobConf = context.getConfiguration();
      if(jobConf.get("unique").equals("1")) {
        unique = true;
      }
      
    }
    
    @Override
    public void reduce(Object key, Iterable<Object> values, Context context) 
        throws IOException, InterruptedException {
      if(unique) {
        context.write(key, values.iterator().next());
      } else {
        for(Object value : values) {
          context.write(key, value);
        }      
      }
    }
    
    
  }
  
  public void usage() {
    System.err.println("Usage: SortPartitionSequenceFile inputpath " +
    		"outputpath unique=? keyclass valueclass numpartitions");
    System.err.println("\tinputpath - path to a sequence file or a " +
    		"folder containing sequence files");
    System.err.println("\toutputpath - path to a store the sorted " +
    		"partitioned sequence files");
    System.err.println("\tunique=?:");
    System.err.println("\t\tSet unique=0 for all rows.");
    System.err.print("\t\tSet unique=1 for using only rows with unique " +
    		"join key. ");
    System.err.println("If multiple rows for same join key exists, only " +
    		"the first row is used.");
    System.err.println("\tkeyclass - class name of the sequence file key " +
    		"field");
    System.err.println("\tvalueclass - class name of the sequence file value " +
    		"field");
    System.err.println("\tnumpartitions - number of output partitionss");
  }
  
  public int run(String [] args) throws Exception {
    if(args.length != 6) {
      usage();
      return -1;
    }
    
    String inputPath, outputPath, unique, keyClassName, valueClassName;
    int numReducers = 1;
    Class keyClass, valueClass;
    Configuration conf;
    Job job;
    
    inputPath = args[0];
    outputPath = args[1];
    unique= "";
    if(args[2].equals("unique=0")) {
      unique="0";
    } else if(args[2].equals("unique=1")) {
      unique="1";
    } else {
      System.err.println("Invalid unique flag!");
      usage();
    } 
    keyClassName = args[3];
    valueClassName = args[4];
    
    try {
      numReducers = Integer.parseInt(args[5]);
    } catch (NumberFormatException e) {
      System.err.println("Invalied numpartitions!");
      usage();
      return -1;
    }
    
    try {
    keyClass = Class.forName(keyClassName);
    valueClass = Class.forName(valueClassName);
    } catch (Exception e) {
      System.err.println(e.getMessage());
      System.err.println("Check class Name!");
      usage();
      return -1;
    }
    
    //COnfigure the job
    conf = getConf();
    conf.set("unique", unique);
    job = new Job(conf, "SortPartitionSequenceFile");
    job.setJarByClass(SortPartitionSequenceFile.class);
    job.setMapperClass(MapClass.class);
    job.setReducerClass(ReduceClass.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setMapOutputKeyClass(keyClass);
    job.setMapOutputValueClass(valueClass);
    job.setOutputKeyClass(keyClass);
    job.setOutputValueClass(valueClass);
    job.setNumReduceTasks(numReducers);
    //Add input path to job
    SequenceFileInputFormat.addInputPath(job, new Path(inputPath));
    SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));
    
    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    sLogger.info("Completed in " +
    		((System.currentTimeMillis() - startTime)/1000) + "secs.");
    return 0;
  }
  
  public static void main(String [] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SortPartitionSequenceFile(), args); 
    System.exit(res);
  }

}
