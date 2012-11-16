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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs; 
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
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
 * the same join key, i.e. many-to-many join.
 * This tool takes the following command-line arguments:
 * </p>
 * 
 * <ul>
 * <li>[input-path1] input dataset one path</li>
 * <li>[input-path1] input dataset two path</li>
 * <li>[output-path] output path</li>
 * <li>[num-reducers] number of reducers</li>
 * <li>Optional: -RC - flag to set RawComparator</li>
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


public class DemoReduceSideJoinPig extends Configured implements Tool {
	private static Logger sLogger = Logger.getLogger(DemoReduceSideJoinPig.class);
	
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
			<PairOfLongInt, BinSedesTuple, BinSedesTuple, NullWritable> {
		
		//Statically define tupleOut for object reuse
		private static Tuple tupleOut;

		
		//Buffer for storing the rows from dataset one with a the current join key
		private List setOneBuffer = new ArrayList<Tuple>();
		
		private long oldJoinKey = 0;
		private long currJoinKey;
		
		/*// method to clone a ArrayListWritable of type Text
		private ArrayListWritable<Text> copyArrayList(ArrayListWritable<Text> list) {
			ArrayListWritable<Text> newList = new ArrayListWritable<Text>();
			int listSize = list.size();
			for(int i = 0; i < listSize; i++) {
				Text token = new Text(((Text)list.get(i)).toString());
				newList.add(token);
			}
			return newList;
		}*/
		
		@Override
		public void reduce(PairOfLongInt keyPair, Iterable<BinSedesTuple> tuples, Context context) 
				throws IOException, InterruptedException {
			Iterator<BinSedesTuple> iter = tuples.iterator();
			
			//Array to store the combined columns to the two datasets
			List combinedFields; 
			BinSedesTuple tupleIn;
			currJoinKey = keyPair.getLeftElement();
			
			//Reset the dataset one buffer when a new join key is encountered
			if(oldJoinKey != currJoinKey) {
				oldJoinKey = currJoinKey;
				setOneBuffer = new ArrayList<BinSedesTuple>();
			}
			
			while(iter.hasNext()) {
				tupleIn = (BinSedesTuple)iter.next();
				
				
				if(keyPair.getRightElement() == 0) {
					//If the tuple belongs to dataset one, add it to buffer
					setOneBuffer.add(TUPLE_FACTORY.newTuple(tupleIn.getAll()));
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
							
							//Emit the combined tokens as the key, and null as value
							context.write((BinSedesTuple) tupleOut, NullWritable.get());
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

	
	public DemoReduceSideJoinPig() {
	}
	
	private static int printUsage() {
    System.out.println("usage: [input-path1] [input-path2] [output-path] [num-reducers]");
    System.out.println("usage (withRawComparator): [input-path1] [input-path2] [output-path] [num-reducers] -RC");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	public int run(String[] args) throws Exception {
		if (args.length != 4 || args.length != 5) {
			printUsage();
			return -1;
		}

		String inputPath1 = args[0];
		String inputPath2 = args[1];
		String outputPath = args[2];
		int numReduceTasks = Integer.parseInt(args[3]);
		boolean rawComparatorMode = false;
		if(args.length == 5 && args[4].equals("-RC")) {
		  rawComparatorMode = true;
		}

		sLogger.info("Tool: DemoReduceSideJoinPig");
		sLogger.info(" - input path1: " + inputPath1);
		sLogger.info(" - input path2: " + inputPath2);
		sLogger.info(" - output path: " + outputPath);
		sLogger.info(" - number of reducers: " + numReduceTasks);

		Configuration conf = getConf();
		Job job = new Job(conf, "DemoReduceSideJoinPig");
		job.setJarByClass(DemoReduceSideJoinPig.class);
		job.setNumReduceTasks(numReduceTasks);

		Path setOnePath = new Path(inputPath1);
		Path setTwoPath = new Path(inputPath2);
		Path outputDir = new Path(outputPath);

		MultipleInputs.addInputPath(job, setOnePath, SequenceFileInputFormat.class, SetOneMapClass.class);
		MultipleInputs.addInputPath(job, setTwoPath, SequenceFileInputFormat.class, SetTwoMapClass.class);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(BinSedesTuple.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setMapOutputKeyClass(PairOfLongInt.class);
		job.setMapOutputValueClass(BinSedesTuple.class);

		job.setReducerClass(JoinReduceClass.class);
		
		job.setPartitionerClass(JoinPartitionClass.class);
		
		if(rawComparatorMode) {
		  job.setSortComparatorClass(PairOfLongIntComparator.class);
		}
		// Delete the output directory if it exists already
		FileSystem.get(conf).delete(outputDir, true);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		sLogger.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DemoReduceSideJoinPig(), args);
		System.exit(res);
	}
	

}
