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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.UUID;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.BinSedesTuple;
import org.apache.pig.data.TupleFactory;

/**
 * <p>
 * Demo that packs the text datasets into a SequenceFile as {@link Tuple}
 * objects with complex internal structure. Both input and output are accessed
 * from the HDFS; this file can used as an input to {@link DemoReduceSideJoinPig}. 
 * The input can be a file or a folder. If the input is a folder, the program
 * will read each file in the folder. (The '_logs' and '_SUCCESS' files will 
 * be neglected. All sub-directores will be omitted as well)
 * </p>
 * 
 * <p>
 * The join key column is the SequenceFile key, and pig {@link Tuple} containing  
 * rest of the columns is the SequenceFile value.
 * </p>
 * 
 * 
 * @see DemoUnpackPigTuples
 * @see DemoReduceSideJoinPig
 */
public class DemoPackPigTuples extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(DemoPackPigTuples.class);
	
	private DemoPackPigTuples() {
	}

	// instantiate a tuple factory for creating tuples
	private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

	// instantiate a single tuple
	private static Tuple tuple;

	
	private static class MapClass extends Mapper<LongWritable, Text, 
	    NullWritable, NullWritable> {

    //Object created statically for reuse
    private FileSystem fs;
    private Configuration jobConf;
    private static DemoPackPigTuples tuplesPacker = new DemoPackPigTuples();
    static long cnt = 0;
    ColSpec[] colspecs;
    String schema;
    int joinColumn;
    String outFile;
    SequenceFile.Writer writer;
    
    @Override
    public void setup(Context context) 
        throws IOException, InterruptedException {
      
      jobConf = context.getConfiguration();
      schema = jobConf.get("schema");
      outFile = jobConf.get("outFile");
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
      //Intialize the tuple
      //The join key column is not included in the tuple
      tuple =  TUPLE_FACTORY.newTuple(colspecs.length - 1);
      fs = FileSystem.get(jobConf);
      //Delete file if already exists (created previously by a 
      //task that is failed/killed)
      fs.delete(new Path(outFile), true);
      writer = SequenceFile.createWriter(fs, jobConf, new Path(outFile),
          LongWritable.class, BinSedesTuple.class);

    }
    
    @Override
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
      
      
      long joinKey;
      LongWritable lw = new LongWritable();
      String line;
      
      sLogger.info("Processing file: " + value.toString());
      Path inFile = new Path(value.toString());
      
     
      // read in raw text records, line separated
      DataInputStream d = new DataInputStream(fs.open(inFile));
      BufferedReader data = new BufferedReader(new InputStreamReader(d));
  
      while ((line = data.readLine()) != null) {
        StringTokenizer itr = new StringTokenizer(line);
        
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
        
        //sLogger.info("JoinKey: " + joinKey + " Tuple: " + tuple.toString());
        // write the record
        writer.append(lw, tuple);
        cnt++;
      }
      data.close();
      sLogger.info("Finished processing " + value.toString() + " file.");
    
      
      //Emit the keypair and tuple
      context.write(NullWritable.get(), NullWritable.get());
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
      writer.close(); 
      sLogger.info("Written " + cnt + " to file.");
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
	
  private static void usage() {
    System.err.print("Usage: DemoPackPigTuples infile outfile ");
    System.err.println("joinkey_column columntype ...");
    System.err.println("\tjoinkey_column:");
    System.err.println("\t\tThe column number of the join key column.");
    System.err.print("\t\tExample: DemoPackPigTuples infile outfile ");
    System.err.println("3 i:1:100:u:0 s:3:9:u:0 l:1:100:u:0");
    System.err.print("\t\tHere, the third column will be considered as ");
    System.err.println("the join key");
    System.err.println("\tcolumntype:");
    System.err.println("\t\ti = int");
    System.err.println("\t\tl = long");
    System.err.println("\t\tf = float");
    System.err.println("\t\td = double");
    System.err.println("\t\ts = string");
    System.err.println("\t\tm = map - NOT SUPPORTED YET");
    System.err.println("\t\tbx = bag of x, where x is a columntype. " +
        "BAG - NOT SUPPORTED YET");
    System.exit(-1);
  }
  
  /*
   * Custom InputFormat Class to force single map task
   * by disabling input file spitting.
   */
  public static class UnspilitableSequenceFileInputFormat<K, V> 
      extends SequenceFileInputFormat<K, V> {
    
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
      return false;
    }
    
  }
	
  /**
	 * Runs the demo.
	 */
	  
  public int run (String[] args) throws Exception, FileAlreadyExistsException {
		
		String inFile = args[0];
    String outFile = args[1];
    int joinColumn = 1;
    //Verify third parameter is a number
    try { 
      joinColumn = Integer.parseInt(args[2]); 
    } catch(NumberFormatException e) {
      System.err.println("Invalid joinKey_column Number!");
      usage(); 
    }
    
		String schema = "";
		Text text = new Text();
		Path tmpFile;
		Path tmpOutFile;
		
		sLogger.info("input: " + inFile);
		sLogger.info("output: " + outFile);
		
		for(int i=3;i<args.length;i++) {
		  schema += args[i] + " ";
		}
		
		//For storing the list of files to be processed
    tmpFile = new Path("tmp/pigtuplepacker-" + UUID.randomUUID());
    tmpOutFile = new Path("tmp/pigtuplepacker-" + UUID.randomUUID());
		
    Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		FileStatus input = fs.getFileStatus(new Path(inFile));
		if(fs.exists(new Path(outFile))) {
		  throw new FileAlreadyExistsException("Output file exists already!");
		}
		
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, tmpFile,
				LongWritable.class, Text.class);

    conf.set("schema", schema);
    conf.set("outFile", outFile);
    conf.set("joinColumn", Integer.toString(joinColumn));
    
		LongWritable lw = new LongWritable();
		long cnt = 0;
		FileStatus[] inputFiles;
    
		if(input.isDir()) {
		  //Get hdfs status objects of all files in the input folder
		  inputFiles = fs.listStatus(new Path(inFile));
		} else {
		  //If input is a file, add it as the only element of files list
		  inputFiles = new FileStatus[1];
		  inputFiles[0] = input;
		}
		for(FileStatus file : inputFiles) {
		  if(input.isDir()) {
		    //Skip non-data files genrated by hadoop in the input directory
		    if(file.getPath().toUri().getPath().endsWith("_logs") || 
		        file.getPath().toUri().getPath().endsWith("_SUCCESS") ||
		        file.isDir()) {
		      continue;
		    }
	    }
		  lw.set(cnt);
		  text.set(file.getPath().toUri().getPath());
		  sLogger.info("Added file: " + text.toString());
		  writer.append(lw, text);
		  cnt++;
		}
		writer.close();		
		sLogger.info("Total files to process: " + cnt);
		
		Job job = new Job(conf, "DemoPackPigTuples");
    job.setJarByClass(DemoPackPigTuples.class);

    UnspilitableSequenceFileInputFormat.setInputPaths(job, tmpFile);
    FileOutputFormat.setOutputPath(job, tmpOutFile);
    
    job.setInputFormatClass(UnspilitableSequenceFileInputFormat.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);

    job.setMapperClass(MapClass.class);
    
    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    
    fs.delete(tmpFile, true);
    fs.delete(tmpOutFile, true);
    
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    
    return 0;
	}
	
  
	public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      usage();
    }
    int res = ToolRunner.run(new Configuration(), new DemoPackPigTuples(), args);
    System.exit(res);
	}  

}