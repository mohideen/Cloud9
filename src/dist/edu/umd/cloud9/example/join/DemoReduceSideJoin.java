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

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.lang.Object;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs; 
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.log4j.Logger;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.BinSedesTuple;
import org.apache.pig.data.TupleFactory;

import edu.umd.cloud9.io.pair.PairOfLongInt;


/**
 * <p>
 * Relational Join demo. This Hadoop Tool reads {@link Tuple} objects from two separate 
 * SequenceFile and joins them based on key. Both datasets can have multiple records with
 * the same join key, i.e. for many-to-many join.
 * This tool takes the following command-line arguments:
 * </p>
 * 
 * <ul>
 * <li>[input-path1] input dataset one file/folder path</li>
 * <li>[input-path2] input dataset two file/folder path</li>
 * <li>[output-path] output path</li>
 * <li>[num-reducers] number of reducers</li>
 * </ul>
 * 
 * <p>
 * Two flat text datasets are packed into two SequenceFiles with
 * {@link DemoPackTuples} respectively; each row in the input SequenceFile 
 * will have a join key field and a tuple value field. The tuple Output will have  
 * combined fields from both dataset joined based on the join key. The output SequenceFile
 * can be converted into text file using {@link DemoUnpackTuples}
 * </p>
 * 
 * @see DemoPackTuples
 * @see DemoUnpackTuples
 * 
 * @author Mohamed M. Abdul Rasheed
 */


public class DemoReduceSideJoin extends Configured implements Tool {
	private static Logger sLogger = Logger.getLogger(DemoReduceSideJoin.class);
	
  //instantiate a tuple factory for creating tuples
  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  
	//This map class emits the join key and dataset identifier flag pair as key, 
	// and tuple as value for dataset one
	private static class SetOneMapClass extends Mapper<LongWritable, BinSedesTuple, 
			PairOfLongInt, BinSedesTuple> {
		
		//Object created statically for reuse
		private static final PairOfLongInt keyPair = new PairOfLongInt();
		
		@Override
		public void map(LongWritable key, BinSedesTuple tuple, Context context) 
				throws IOException, InterruptedException {
			
			//Set the join key and '0' flag for dataset one
			keyPair.set(key.get(), 0);
			
			//Emit the keypair and tuple
			context.write(keyPair, tuple);
		}
	}
	
	//This map class emits the join key and dataset identifier flag pair as key, 
	// and tuple as value for dataset two
	private static class SetTwoMapClass extends Mapper<LongWritable, BinSedesTuple, 
			PairOfLongInt, BinSedesTuple> {
		
		//Object created statically for reuse
		private static final PairOfLongInt keyPair = new PairOfLongInt();
		
		@Override
		public void map(LongWritable key, BinSedesTuple tuple, Context context) 
				throws IOException, InterruptedException {
			
			//Set the join key and '1' flag for dataset two
			keyPair.set(key.get(), 1);
			
			//Emit the keypair and tuple
			context.write(keyPair, tuple);
		}
	}
	
	//Reduce class joins the datasets based on the join key. 
	private static class JoinReduceClass extends Reducer
			<PairOfLongInt, BinSedesTuple, LongWritable, BinSedesTuple> {
		
		//Statically define tupleOut for object reuse
		private static Tuple tupleOut;
		private static LongWritable lw = new LongWritable();

		
		//Buffer for storing the rows from dataset one with a the current join key
		private List<BinSedesTuple> setOneBuffer = new ArrayList<BinSedesTuple>();
		
		private long oldJoinKey = -1;
		private long currJoinKey = -1;
		
		@Override
		public void reduce(PairOfLongInt keyPair, Iterable<BinSedesTuple> tuples, Context context) 
				throws IOException, InterruptedException {
			Iterator<BinSedesTuple> iter = tuples.iterator();
			
			//Array to store the combined columns to the two datasets
			List<Object> combinedFields; 
			BinSedesTuple tupleIn;
			currJoinKey = keyPair.getLeftElement();
			
			//Reset the dataset one buffer when a new join key is encountered
			if(oldJoinKey != currJoinKey) {
				oldJoinKey = currJoinKey;
				setOneBuffer.clear();
			}
			
			while(iter.hasNext()) {
				tupleIn = (BinSedesTuple)iter.next();
				
				
				if(keyPair.getRightElement() == 0) {
					//If the tuple belongs to dataset one, add it to buffer
					setOneBuffer.add((BinSedesTuple)TUPLE_FACTORY.newTuple(tupleIn.getAll()));
				} else {
					//If the tuple belongs to dataset two, join it with the
					//corresponding rows from dataset one with same join key
					if(!setOneBuffer.isEmpty()) {
						for(Iterator<BinSedesTuple> i = setOneBuffer.iterator(); i.hasNext(); ) {
						  combinedFields = new ArrayList<Object>();
							
							//Add columns of the row from dataset one to the combined list
						  combinedFields.addAll(i.next().getAll());
							//Add columns of the row from dataset two to the combined list
						  combinedFields.addAll(tupleIn.getAll());
							
							//Set the combined tokens to the output tuple
							tupleOut = TUPLE_FACTORY.newTuple(combinedFields);
							lw.set(currJoinKey);

							//Emit the combined tokens as the key, and null as value
							context.write(lw, (BinSedesTuple) tupleOut);
						}
					}
				}
			} 
			
		}		
	}
	
	private static class JoinPartitionClass extends Partitioner<PairOfLongInt, Tuple> {
		
		@Override
		public int getPartition(PairOfLongInt key, Tuple tuple, int numReduceTasks) {
			return ((int)key.getLeftElement()) % numReduceTasks;
		}
	}
	
	//This is redundant as the PairOfLongInt already supports unserialized comparison.
	public static class PairOfLongIntComparator extends WritableComparator {
	  protected PairOfLongIntComparator() {
	    super(PairOfLongInt.class);
	  }
	  
	  @Override
	  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	    long lp1 = readLong(b1, s1);
	    long lp2 = readLong(b2, s2);
	    
	    int comp = 0;
	    if(lp1 < lp2) {
	      comp = -1;
	    } 
	    if(lp1 > lp2) {
	      comp = 1;
	    }
	    if(0 != comp)
	      return comp;
	    
	    int ip1 = readInt(b1, s1+8);
	    int ip2 = readInt(b2, s2+8);
	    if(ip1 < ip2) {
        comp = -1;
      } 
      if(ip1 > ip2) {
        comp = 1;
      }
	    
	    return comp;
	  }
	}

	
	public DemoReduceSideJoin() {
	}
	
	private void usage() {
    System.out.println("usage: [input-path1] [input-path2] [output-path] " +
    		"[num-reducers]");
    System.out.println("Use the smaller of the two datasets as input one.");
    ToolRunner.printGenericCommandUsage(System.out);
	}
	
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			usage();
			return -1;
		}

		String inputPath1 = args[0];
		String inputPath2 = args[1];
		String outputPath = args[2];
		int numReduceTasks = Integer.parseInt(args[3]);

		sLogger.info("Tool: DemoReduceSideJoin");
		sLogger.info(" - input path1: " + inputPath1);
		sLogger.info(" - input path2: " + inputPath2);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - number of reducers: " + numReduceTasks);

		Configuration conf = getConf();
    Job job = new Job(conf, "DemoReduceSideJoin");
		job.setJarByClass(DemoReduceSideJoin.class);
		job.setNumReduceTasks(numReduceTasks);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(BinSedesTuple.class);
    job.setMapOutputKeyClass(PairOfLongInt.class);
    job.setMapOutputValueClass(BinSedesTuple.class);
    job.setReducerClass(JoinReduceClass.class);
    job.setPartitionerClass(JoinPartitionClass.class);
    
		Path setOnePath = new Path(inputPath1);
		Path setTwoPath = new Path(inputPath2);
		Path outputDir = new Path(outputPath);
		
    //Add input one files to job
    MultipleInputs.addInputPath(job, setOnePath, SequenceFileInputFormat.class, SetOneMapClass.class);
    MultipleInputs.addInputPath(job, setTwoPath, SequenceFileInputFormat.class, SetTwoMapClass.class);
    		
		FileOutputFormat.setOutputPath(job, outputDir);
		
		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DemoReduceSideJoin(), args);
		System.exit(res);
	}
	

}
