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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import org.apache.pig.data.BinSedesTuple;

/**
 * <p>
 * Demo that unpacks the tuples from a SequenceFile into a flat text file. 
 * Both input and output are accessed from the HDFS; The output of 
 * {@link DemoReduceSideJoin} / {@link DemoMapSideJoin} can be used as the 
 * input to this  program. 
 * This tool takes the following command-line arguments:
 * </p>
 * 
 * <ul>
 * <li>[input-path] input path</li>
 * <li>[output-path] output path</li>
 * </ul>
 * 
 * 
 * @see DemoPackTuples
 * @see DemoReduceSideJoin
 * @see DemoMapSideJoin
 */
public class DemoUnpackTuples extends Configured implements Tool {
  private static final Logger sLogger = Logger.getLogger(DemoUnpackTuples.class);

  private DemoUnpackTuples() {
  }
  
  public static class MapClass extends Mapper<LongWritable, BinSedesTuple, 
      NullWritable, Text> {
    private static Text text = new Text();
    @Override
    public void map(LongWritable key, BinSedesTuple value, Context context) 
        throws IOException, InterruptedException {
      text.set(key.get() + "\t" + value.toDelimitedString("\t"));
      context.write(NullWritable.get(), text);
      
    }
  }
  
  /**
   * Runs the demo.
   */
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("usage: [input-path] [output-path]");
      return -1;
    }
    
    Path input = new Path(args[0]);
    Path output = new Path(args[1]);
    
    Configuration conf = getConf();
    Job job = new Job(conf, "DemoUnpackTuples");
    job.setJarByClass(DemoUnpackTuples.class);
    
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(0);
    job.setMapperClass(MapClass.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    SequenceFileInputFormat.addInputPath(job, input);
    
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, output);
    
    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    sLogger.info("Job completed in " + 
          (System.currentTimeMillis() - startTime)/1000 + "seconds");
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new DemoUnpackTuples(), args);
    System.exit(res);
  }
}