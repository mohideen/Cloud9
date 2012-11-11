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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.BinSedesTuple;
import org.apache.pig.data.TupleFactory;

/**
 * <p>
 * Demo that packs the text datasets into a SequenceFile as {@link Tuple}
 * objects with complex internal structure. Both input and output are accessed
 * from the HDFS; this file can used as an input to {@link DemoReduceSideJoin}. 
 * The input can be a file or a folder. If the input is a folder, the program
 * will read each file in the folder. (The '_logs' and '_SUCCESS' files will 
 * be neglected. All sub-directores will be omitted as well)
 * </p>
 * 
 * <p>
 * Each value in the SequenceFile is a combintion of a long value and a tuple with 
 * two fields:
 * </p>
 * 
 * <ul>
 * 
 * <li>the first field of the tuple is an Integer with the field name "tupleId";
 * its value is the assined incrementally from 1 to the total number of 
 * rows in the text dataset.</li>
 * 
 * <li>the second field of the tuple is a ListWritable<Text> with the field name
 * "columns"; its value is a list of text that comprise the columns of the record.
 * </li>
 * 
 * </ul>
 * 
 * @see DemoUnpackTuples
 * @see DemoReduceSideJoin
 */
public class DemoPackPigTuples {
	private static final Logger sLogger = Logger.getLogger(DemoPackPigTuples.class);
	ColSpec[] colspecs;
  
	private DemoPackPigTuples() {
	}

	// instantiate a tuple factory for creating tuples
	private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

	// instantiate a single tuple
	private static Tuple tuple;

	public static void main(String[] args) throws IOException {
    if (args.length < 3) {
      usage();
    }
    DemoPackPigTuples packer = new DemoPackPigTuples();
    packer.run(args);
	}
	
  /**
	 * Runs the demo.
	 */
	  
  public void run (String[] args) throws IOException {
		
		String infile = args[0];
		String outfile = args[1];
	  
		//hdfs status of the input file
		sLogger.info("input: " + infile);
		sLogger.info("output: " + outfile);
		
		colspecs = new ColSpec[args.length - 2];
		for(int i=2;i<args.length;i++) {
		  colspecs[i-2] = new ColSpec(args[i]);
		}
		
		tuple =  TUPLE_FACTORY.newTuple(colspecs.length - 1);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FileStatus input = fs.getFileStatus(new Path(infile));
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(outfile),
				LongWritable.class, BinSedesTuple.class);

		
		LongWritable l = new LongWritable();
		long cnt = 0;
		long joinKey;
		
		String line;
		FileStatus[] inputFiles;
    
		if(input.isDir()) {
		  //Get hdfs status objects of all files in the input folder
		  inputFiles = fs.listStatus(new Path(infile));
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
		  // read in raw text records, line separated
		  DataInputStream d = new DataInputStream(fs.open(file.getPath()));
      BufferedReader data = new BufferedReader(new InputStreamReader(d));
  
  		while ((line = data.readLine()) != null) {
  			StringTokenizer itr = new StringTokenizer(line);
  			joinKey = Long.parseLong(itr.nextToken());
  			//while (itr.hasMoreTokens()) {
  			//	tokens.add(new Text(itr.nextToken()));
  			//}
  			//Add count as the first tuple field
  			//tuple.set(0, cnt + 1);
  		  //Set the tuple data fields.
  			for(int i = 1; i < colspecs.length; i++) {
  			  if(itr.hasMoreTokens()) {
  			    switch(colspecs[i].datatype) {
            case DataType.INTEGER: 
              tuple.set(i-1, DataType.toInteger(itr.nextToken()));
              break;
            case DataType.LONG: 
              tuple.set(i-1, DataType.toLong(itr.nextToken()));
              break;
            case DataType.FLOAT: 
              tuple.set(i-1, DataType.toFloat(itr.nextToken()));
              break;
            case DataType.DOUBLE: 
              tuple.set(i-1, DataType.toDouble(itr.nextToken()));
              break;
            case DataType.CHARARRAY: 
              tuple.set(i-1, DataType.toString(itr.nextToken()));
              break;
  			    }
  			    
  			  } else {
  			    System.err.println("Data file schema does not match the colspec!");
  			    System.exit(-1);
  			  }
  			}
  			l.set(joinKey);
  			//sLogger.info("JoinKey: " + joinKey + " Tuple: " + tuple.toString());
  			// write the record
  			writer.append(l, tuple);
  			cnt++;
  		}
  		data.close();
  		sLogger.info("Finished processing " + 
  		    file.getPath().toUri().getPath() + " file.");
		}
		writer.close();
		
		sLogger.info("Wrote " + cnt + " records.");
	}
	
	
	protected class ColSpec {
    String arg;
    byte datatype;
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
    System.err.println("Usage: DemoPackPigTuples infile outfile coltype ...");
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
  

}