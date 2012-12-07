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
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.BinSedesTuple;
import org.apache.pig.data.TupleFactory;

/**
 * <p>
 * Demo that packs the text datasets into SequenceFiles containing {@link Tuple}
 * objects with complex internal structure. Both input and output are accessed
 * from the HDFS; The schema of the dataset and the join key column should be given
 * as command line inputs. The output file can used as an input to 
 * {@link DemoReduceSideJoin}. 
 * This tool takes the following command-line arguments:
 * </p>
 * 
 * <ul>
 * <li>[input-path1] input path</li>
 * <li>[output-path] output path</li>
 * <li>[joinkey-column] column number of the join key field</li>
 * <li>[schema] schema of the input dataset</li>
 * </ul>
 * 
 * <p>
 * The join key column of the dataset is writtem as the SequenceFile key, 
 * and a pig {@link Tuple} containing rest of the columns is written as
 * SequenceFile value. 
 * </p>
 * 
 * 
 * @see DemoUnpackTuples
 * @see DemoReduceSideJoin
 */
public class DemoPackTuples extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(DemoPackTuples.class);
	
	private DemoPackTuples() {
	}

	private static class MapClass extends Mapper<LongWritable, Text, 
	    LongWritable, BinSedesTuple> {

    //Objects created statically for reuse
	  //instantiate a tuple factory for creating tuples
	  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
	  //tuple 
	  private static Tuple tuple;
	  private Configuration jobConf;
    private static DemoPackTuples tuplesPacker = new DemoPackTuples();
    private ColSpec[] colspecs;
    private String schema;
    private int joinColumn;
    
    @Override
    public void setup(Context context) 
        throws IOException, InterruptedException {
      
      jobConf = context.getConfiguration();
      schema = jobConf.get("schema");
      joinColumn = Integer.parseInt(jobConf.get("joinColumn"));
      StringTokenizer tokens = new StringTokenizer(schema, " "); 
      //Process schema argument into ColSpec classes
      colspecs = new ColSpec[tokens.countTokens()];
      for (int i = 0; tokens.hasMoreElements(); i++) {  
        colspecs[i] = tuplesPacker.new ColSpec(tokens.nextToken());
        //Check if the current column is the join key column
        if(i == (joinColumn-1)) {
          colspecs[i].isJoinKey = true;
        }
      }
      //Initialize the tuple
      //The join key column is not included in the tuple
      tuple =  TUPLE_FACTORY.newTuple(colspecs.length - 1);
      
    }
    
    @Override
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
      
      long joinKey;
      LongWritable lw = new LongWritable();
      StringTokenizer itr = new StringTokenizer(value.toString());
      
      nextColumn:
      for(int i = 0, fieldCount = 0; i < colspecs.length; i++) {
        if(itr.hasMoreTokens()) {
          if(colspecs[i].isJoinKey) {
            joinKey = Long.parseLong(itr.nextToken());
            lw.set(joinKey);
            continue nextColumn;
          }
          switch(colspecs[i].datatype) {
          case DataType.INTEGER: 
            tuple.set(fieldCount, DataType.toInteger(itr.nextToken()));
            break;
          case DataType.LONG: 
            tuple.set(fieldCount, DataType.toLong(itr.nextToken()));
            break;
          case DataType.FLOAT: 
            tuple.set(fieldCount, DataType.toFloat(itr.nextToken()));
            break;
          case DataType.DOUBLE: 
            tuple.set(fieldCount, DataType.toDouble(itr.nextToken()));
            break;
          case DataType.CHARARRAY: 
            tuple.set(fieldCount, DataType.toString(itr.nextToken()));
            break;
          }
          
        } else {
          System.err.println("Data file schema does not match the colspec!");
          System.exit(-1);
        }
        fieldCount++;
      }
      
      // write the record
      context.write(lw, (BinSedesTuple)tuple);
           
    }
  }
	
	protected class ColSpec {
    String arg;
    byte datatype;
    boolean isJoinKey = false;
    ColSpec contained;

    public ColSpec(String arg) {
      this.arg = arg;
      if (arg.length() != 1 && arg.length() != 2) {
        System.err.println("Colspec [" + arg + "] format incorrect"); 
        usage();
      }

      switch (arg.charAt(0)) {
        case 'i': datatype = DataType.INTEGER; break;
        case 'l': datatype = DataType.LONG; break;
        case 'f': datatype = DataType.FLOAT; break;
        case 'd': datatype = DataType.DOUBLE; break;
        case 's': datatype = DataType.CHARARRAY; break;
        case 'm': datatype = DataType.MAP; 
          System.err.println("MAP column type NOT SPPORTED");
          usage();
          break;
        case 'b':
          datatype = DataType.BAG;
          contained = new ColSpec(arg.substring(1));
          System.err.println("BAG column type NOT SPPORTED");
          usage();
          return;
        default: 
          System.err.println("Don't know column type " +
              arg.charAt(0));
          usage();
          break;
      }
      
      contained = null;
    }
  }
	
  private void usage() {
    System.err.print("Usage: DemoPackTuples inpath outpath ");
    System.err.println("joinkey_column columntype ...");
    System.err.println("\tjoinkey_column:");
    System.err.println("\t\tThe column number of the join key column.");
    System.err.println("\tcolumntype:");
    System.err.println("\t\ti = int");
    System.err.println("\t\tl = long");
    System.err.println("\t\tf = float");
    System.err.println("\t\td = double");
    System.err.println("\t\ts = string");
    System.err.println("\t\tm = map - NOT SUPPORTED YET");
    System.err.println("\t\tbx = bag of x, where x is a columntype. " +
        "BAG - NOT SUPPORTED YET");
    System.err.print("\tExample: DemoPackTuples infile outfile ");
    System.err.println("3 i s l");
    System.err.print("\t\tHere, the third column will be considered as ");
    System.err.println("the join key");
    ToolRunner.printGenericCommandUsage(System.out);
  }
  
  /**
	 * Runs the demo.
	 */
	  
  public int run (String[] args) throws Exception {
		
    if (args.length < 3) {
      usage();
      return -1;
    }
		String inputPath = args[0];
    String outputPath = args[1];
    int joinColumn = 1;
    String schema = "";
    
    //Verify third parameter is a number
    try { 
      joinColumn = Integer.parseInt(args[2]); 
    } catch(NumberFormatException e) {
      System.err.println("Invalid joinKey_column Number!");
      usage(); 
      return -1;
    }
    //Collect the schema to a string
		for(int i=3;i<args.length;i++) {
		  schema += args[i] + " ";
		}
		
		sLogger.info("Tool: DemoPackTuples");
		sLogger.info("input: " + inputPath);
    sLogger.info("output: " + outputPath);
    sLogger.info("join-cloumn: " + joinColumn);
    sLogger.info("schema: " + schema);
		
		Configuration conf = getConf();	
		//Add the schema to job configuration. (To be read by map tasks)
		conf.set("schema", schema);
    conf.set("joinColumn", Integer.toString(joinColumn));
    
    Job job = new Job(conf, "DemoPackTuples");
    job.setJarByClass(DemoPackTuples.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(BinSedesTuple.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(BinSedesTuple.class);
    job.setMapperClass(MapClass.class);
    job.setNumReduceTasks(0);
    
    TextInputFormat.addInputPath(job, new Path(inputPath));
    SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));
    
    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);    
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    
    return 0;
	}
	
	public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new DemoPackTuples(), args);
    System.exit(res);
	}  

}
