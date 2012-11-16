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
import java.util.LinkedList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configuration;

import org.apache.log4j.Logger;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import edu.umd.cloud9.io.Tuple;
import edu.umd.cloud9.io.Schema;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfLongInt;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * <p>
 * Relational Join demo. This Hadoop Tool reads {@link Tuple} objects from two separate 
 * SequenceFile and joins them based on key. Both datasets can have multiple records with
 * the same join key, i.e. many-to-many join.
 * This tool takes the following command-line arguments:
 * </p>
 * 
 * <ul>
 * <li>[input-path1] input dataset one path</li>
 * <li>[input-path1] input dataset two path</li>
 * <li>[output-path] output path</li>
 * <li>[num-reducers] number of reducers</li>
 * </ul>
 * 
 * <p>
 * Two flat text datasets are packed into two SequenceFiles with
 * {@link DemoPackTuples} respectively; each row in the input SequenceFile 
 * will have a join key field and a tuple field. The input tuple will have 
 * a unique id column and a text array column. The tuple Output will have a 
 * single combined text array column consisting of text array columns from 
 * both datasets joined based on the join key. The output SequenceFile can 
 * be converted into text file using {@link DemoUnpackTuples}
 * </p>
 * 
 * @see DemoPackTuples
 * @see DemoUnpackTuples
 * 
 * @author Mohamed M. Abdul Rasheed
 */


public class DemoReduceSideJoin extends Configured implements Tool {
	private static Logger sLogger = Logger.getLogger(DemoReduceSideJoin.class);
	
	//The schema for storing the joined data
	private static final Schema OUTPUT_SCHEMA = new Schema();
	static {
		OUTPUT_SCHEMA.addField("combined", ArrayListWritable.class);
	}
	
	//This map class emits the join key and dataset identifier flag pair as key, 
	// and tuple as value for dataset one
	private static class SetOneMapClass extends Mapper<LongWritable, Tuple, 
			PairOfLongInt, Tuple> {
		
		//Object created statically for reuse
		private static final PairOfLongInt keyPair = new PairOfLongInt();
		
		@Override
		public void map(LongWritable key, Tuple tuple, Context context) 
				throws IOException, InterruptedException {
			
			//Set the join key and '0' flag for dataset one
			keyPair.set(key.get(), 0);
			
			//Emit the keypair and tuple
			context.write(keyPair, tuple);
		}
	}
	
	//This map class emits the join key and dataset identifier flag pair as key, 
	// and tuple as value for dataset two
	private static class SetTwoMapClass extends Mapper<LongWritable, Tuple, 
			PairOfLongInt, Tuple> {
		
		//Object created statically for reuse
		private static final PairOfLongInt keyPair = new PairOfLongInt();
		
		@Override
		public void map(LongWritable key, Tuple tuple, Context context) 
				throws IOException, InterruptedException {
			
			//Set the join key and '1' flag for dataset two
			keyPair.set(key.get(), 1);
			
			//Emit the keypair and tuple
			context.write(keyPair, tuple);
		}
	}
	
	//Reduce class joins the datasets based on the join key. 
	private static class JoinReduceClass extends Reducer
			<PairOfLongInt, Tuple, Tuple, NullWritable> {
		
		//Statically define tupleOut for object reuse
		private static Tuple tupleOut = OUTPUT_SCHEMA.instantiate();
		
		//Buffer for storing the rows from dataset one with a the current join key
		private List<ArrayListWritable<Text>> setOneBuffer = new LinkedList<ArrayListWritable<Text>>();
		
		private long oldJoinKey = 0;
		private long currJoinKey;
		
		// method to clone a ArrayListWritable of type Text
		private ArrayListWritable<Text> copyArrayList(ArrayListWritable<Text> list) {
			ArrayListWritable<Text> newList = new ArrayListWritable<Text>();
			int listSize = list.size();
			for(int i = 0; i < listSize; i++) {
				Text token = new Text(((Text)list.get(i)).toString());
				newList.add(token);
			}
			return newList;
		}
		
		@Override
		public void reduce(PairOfLongInt keyPair, Iterable<Tuple> tuples, Context context) 
				throws IOException, InterruptedException {
			Iterator<Tuple> iter = tuples.iterator();
			
			//Array to store the combined columns to the two datasets
			ArrayListWritable<Text> combinedTokens; 
			Tuple tupleIn;
			currJoinKey = keyPair.getLeftElement();
			
			//Reset the dataset one buffer when a new join key is encountered
			if(oldJoinKey != currJoinKey) {
				oldJoinKey = currJoinKey;
				setOneBuffer = new LinkedList<ArrayListWritable<Text>>();
			}
			
			while(iter.hasNext()) {
				tupleIn = (Tuple)iter.next();
				
				
				if(keyPair.getRightElement() == 0) {
					//If the tuple belongs to dataset one, add it to buffer
					ArrayListWritable<Text> setOneTokens = 
							copyArrayList((ArrayListWritable<Text>)(tupleIn.get(1))); 
					setOneBuffer.add(setOneTokens);
				} else {
					//If the tuple belongs to dataset two, join it with the
					//corresponding rows from dataset one with same join key
					if(!setOneBuffer.isEmpty()) {
						for(Iterator<ArrayListWritable<Text>> i = setOneBuffer.iterator(); i.hasNext(); ) {
							combinedTokens = new ArrayListWritable<Text>();
							
							//Add columns of the row from dataset one to the combined list
							combinedTokens.addAll((((ArrayListWritable<Text>)i.next())));
							//Add columns of the row from dataset two to the combined list
							combinedTokens.addAll((((ArrayListWritable<Text>)(tupleIn.get(1)))));
							
							//Set the combined tokens to the output tuple
							tupleOut.set("combined", combinedTokens);
							
							//Emit the combined tokens as the key, and null as value
							context.write(tupleOut, NullWritable.get());
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
	
	public DemoReduceSideJoin() {
	}
	
	private static int printUsage() {
		System.out.println("usage: [input-path1] [input-path2] [output-path] [num-reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			printUsage();
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

		Configuration conf = new Configuration();
		Job job = new Job(conf, "DemoReduceSideJoin");
		job.setJarByClass(DemoReduceSideJoin.class);
		job.setNumReduceTasks(numReduceTasks);

		Path setOnePath = new Path(inputPath1);
		Path setTwoPath = new Path(inputPath2);
		Path outputDir = new Path(outputPath);

		MultipleInputs.addInputPath(job, setOnePath, SequenceFileInputFormat.class, SetOneMapClass.class);
		MultipleInputs.addInputPath(job, setTwoPath, SequenceFileInputFormat.class, SetTwoMapClass.class);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(Tuple.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setMapOutputKeyClass(PairOfLongInt.class);
		job.setMapOutputValueClass(Tuple.class);

		job.setReducerClass(JoinReduceClass.class);
		job.setPartitionerClass(JoinPartitionClass.class);

		// Delete the output directory if it exists already
		FileSystem.get(conf).delete(outputDir, true);

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