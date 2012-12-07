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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Reader;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;


import edu.umd.cloud9.mapreduce.lib.input.NonSplitableSequenceFileInputFormat;

import org.apache.pig.data.BinSedesTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


/**
 * <p>
 * Relational Mapside Join demo. This Hadoop Tool reads {@link Tuple} objects 
 * from two separate SequenceFile and joins them based on their join key. The 
 * datasets should be a sorted and partitioned by the join key. Both datasets 
 * can have multiple records with the same join key, i.e. for many-to-many 
 * join. The second dataset is assumed to be the larger dataset and it set as 
 * the Map input. The other dataset will be read from the file system 
 * by respective map tasks. 
 * This tool takes the following command-line arguments:
 * </p>
 * 
 * <ul>
 * <li>[input-path1] input dataset one path</li>
 * <li>[input-path1] input dataset two path</li>
 * <li>[output-path] output path</li>
 * </ul>
 * 
 * <p>
 * Input datasets should first be packed a tuples using the 
 * {@link DemoPackTuples} tool, and sorted/partitioned using 
 * {@link SortPartitionSequenceFile}. The output SequenceFile
 * can be converted back into text file using {@link DemoUnpackTuples}
 * </p>
 * 
 * @see DemoPackTuples
 * @see SortPartitionSequenceFile
 * @see DemoUnpackTuples
 * 
 * @author Mohamed M. Abdul Rasheed
 */

public class DemoMapSideJoin extends Configured implements Tool {
  
  private static Logger sLogger = Logger.getLogger(DemoMapSideJoin.class);
  
  public static class MapClass extends Mapper<LongWritable, BinSedesTuple, LongWritable, BinSedesTuple> {
    
    private FileSystem fs;
    private Configuration conf;
    private static Reader inputOneReader;
    private static Writable inputOneKey, inputOneValue;
    private static long currentInputOneKey = -1;
    private static long setOneBufferKey = -1;
    private static List<Tuple> setOneBuffer = new ArrayList<Tuple>();
    private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
    private static Tuple tupleOut;
    private static LongWritable lw = new LongWritable();
    private static List<Object> combinedFields = new ArrayList<Object>();
    
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      conf = context.getConfiguration();
      fs = FileSystem.get(conf);
      String inputOneDir = conf.get("inputPath1");
      String inputSplitFileName = ((FileSplit) context.getInputSplit()).getPath().getName();
      //Set the first input file corresponding the current map task's input split file name
      String firstInput = inputOneDir + "/" +  inputSplitFileName; 
      sLogger.info("Map Task for :" + inputSplitFileName);
      sLogger.info("Opening matching setone file from fs :" + firstInput);
      inputOneReader = new Reader(fs, new Path(firstInput), conf);
      inputOneKey = (Writable) ReflectionUtils.newInstance(inputOneReader.getKeyClass(), conf);
      inputOneValue = (Writable) ReflectionUtils.newInstance(inputOneReader.getValueClass(), conf);
      
    }
    
    @Override
    public void map(LongWritable key, BinSedesTuple value, Context context) 
        throws IOException, InterruptedException {
      long inputTwoKey = key.get();
      
      //Make dataset one pointer to be equal or greater than dataset two pointer.
      while(currentInputOneKey < inputTwoKey) {
        if(inputOneReader.next(inputOneKey, inputOneValue)) {
          currentInputOneKey = ((LongWritable) inputOneKey).get();
        } else {
          break;
        }
      }
      
      //Clear buffer if the dataset two key has changed
      if(setOneBufferKey != inputTwoKey) {
        setOneBuffer.clear();
      }
      
      //Add dataset one values with same key as dataset two to buffer
      //(to facilitate many to many join)
      if(setOneBuffer.isEmpty()) {
        while(currentInputOneKey == inputTwoKey) {
          setOneBufferKey = currentInputOneKey;
          setOneBuffer.add(TUPLE_FACTORY.newTuple(((BinSedesTuple) inputOneValue).getAll()));
          if(inputOneReader.next(inputOneKey, inputOneValue)) {
            currentInputOneKey = ((LongWritable) inputOneKey).get();
          } else {
            currentInputOneKey = -1;
          }
        }
      }
      
      //If dataset two key matched the buffer key, join the each value in 
      //buffer with dataset two value and write to context
      if(inputTwoKey == setOneBufferKey) {
        lw.set(setOneBufferKey);
        for(Iterator<Tuple> iter = setOneBuffer.iterator(); iter.hasNext(); ) {
          combinedFields.clear();
          
          //Add columns of the row from dataset one to the combined list
          combinedFields.addAll(((Tuple) iter.next()).getAll());
          //Add columns of the row from dataset two to the combined list
          combinedFields.addAll(value.getAll());
          tupleOut = TUPLE_FACTORY.newTuple(combinedFields);
          context.write(lw, (BinSedesTuple) tupleOut);
        }
      } 
      
      
    }
    
    @Override
    public void cleanup(Context context) throws IOException {
      inputOneReader.close();
    }
    
  }

  
  public void usage() {
    System.err.println("Usage: DemoMapSideJoinPig input-path1 input-path2 output-path");
    System.err.println("\tinput-path1: Folder containing sorted partitioned dataset one. (Smaller dataset)");
    System.err.println("\tinput-path2: Folder containing sorted partitioned dataset two. (Larger dataset)");
    System.err.println("\toutput-path: Path to store the joined output.");
    System.err.println("NOTE: Both datasets must have sorted/partitioned by same key and corresponding partitions should have identical names.");
    ToolRunner.printGenericCommandUsage(System.out);
  }
  
  public int run(String[] args) throws Exception {
    if(args.length != 3) {
      usage();
      return -1;
    }
    String inputPath1 = args[0];
    String inputPath2 = args[1];
    String outputPath = args[2];

    sLogger.info("Tool: DemoMapSideJoin");
    sLogger.info(" - input path1: " + inputPath1);
    sLogger.info(" - input path2: " + inputPath2);
    sLogger.info(" - output path: " + outputPath);
    
    Configuration conf = getConf();
    conf.set("inputPath1", inputPath1);
    
    Job job = new Job(conf, "DemoMapSideJoin");
    job.setJarByClass(DemoMapSideJoin.class);
    job.setMapperClass(MapClass.class);
    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(BinSedesTuple.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(BinSedesTuple.class);
    job.setNumReduceTasks(0);
    NonSplitableSequenceFileInputFormat.addInputPath(job, new Path(inputPath2));
    SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));
    
    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    sLogger.info("Completed in " +
        ((System.currentTimeMillis() - startTime)/1000) + "secs.");
    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new DemoMapSideJoin(), args);
    System.exit(res);
  }

}
