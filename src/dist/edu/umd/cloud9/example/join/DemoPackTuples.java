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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.File;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.Schema;
import edu.umd.cloud9.io.Tuple;
import edu.umd.cloud9.io.array.ArrayListWritable;

/**
 * <p>
 * Demo that packs the text datasets into a SequenceFile as {@link Tuple}
 * objects with complex internal structure. The records are stored in a local
 * SequenceFile; this file can used as an input to {@link DemoReduceSideJoin}. 
 * The input can be a file or a folder. If the input is a folder, the program
 * will read each file in the folder. (The '_logs' and '_SUCCESS' files will 
 * be neglected.)
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
public class DemoPackTuples {
	private static final Logger sLogger = Logger.getLogger(DemoPackTuples.class);

	private DemoPackTuples() {
	}

	// define the tuple schema for the input record
	private static final Schema RECORD_SCHEMA = new Schema();
	static {
		RECORD_SCHEMA.addField("tupleId", Long.class);
		RECORD_SCHEMA.addField("columns", ArrayListWritable.class, "");
	}

	// instantiate a single tuple
	private static Tuple tuple = RECORD_SCHEMA.instantiate();

	/**
	 * Runs the demo.
	 */
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("usage: [input-file/folder-name] [output] ");
			System.exit(-1);
		}

		String infile = args[0];
		String outfile = args[1];
	
		File input = new File(infile);
    String inputFiles[];
		sLogger.info("input: " + infile);
		sLogger.info("output: " + outfile);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(outfile),
				LongWritable.class, Tuple.class);

		
		LongWritable l = new LongWritable();
		long cnt = 0;
		long joinKey;
		
		String line;
		
		if(input.isDirectory()) {
		  inputFiles = input.list();
		  if(infile.endsWith("/")) {
		    infile = infile.substring(0, infile.length() -1);
		  }
		} else {
		  inputFiles = new String[1];
		  inputFiles[0] = infile;
		}
		for(String file : inputFiles) {
		  if(input.isDirectory()) {
		    //Skip non-data files genrated by hadoop in the input directory
		    if(file.equals("_logs") || file.equals("_SUCCESS")) {
		      continue;
		    }
		    file = infile + '/' + file;
	    } 
		  // read in raw text records, line separated
      BufferedReader data = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
  
  		while ((line = data.readLine()) != null) {
  			ArrayListWritable<Text> tokens = new ArrayListWritable<Text>();
  			StringTokenizer itr = new StringTokenizer(line);
  			joinKey = Long.parseLong(itr.nextToken());
  			while (itr.hasMoreTokens()) {
  				tokens.add(new Text(itr.nextToken()));
  			}
  
  			//Set the tuple data fields.
  			tuple.set("tupleId", cnt + 1);
  			tuple.set("columns", tokens);
  			l.set(joinKey);
  			// write the record
  			writer.append(l, tuple);
  			cnt++;
  		}
  		data.close();
  		sLogger.info("Finished processing " + file + " file.");
		}
		writer.close();
		
		sLogger.info("Wrote " + cnt + " records.");
	}
}