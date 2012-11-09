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


import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.Tuple;
import edu.umd.cloud9.io.array.ArrayListWritable;

/**
 * <p>
 * Demo that unpacks the tuples from a SequenceFile into a flat text file. 
 * The output of {@link DemoReduceSideJoin} can be used as the input to this
 * program.
 * </p>
 * 
 * <p>
 * Each value in the SequenceFile is a tuple with a single field. the  
 * field of the tuple is a ListWritable<Text>; 
 * </p>
 * 
 * @see DemoPackTuples
 * @see DemoReduceSideJoin
 */
public class DemoUnpackTuples {
	private static final Logger sLogger = Logger.getLogger(DemoUnpackTuples.class);

	private DemoUnpackTuples() {
	}
	/**
	 * Runs the demo.
	 */
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("usage: [input-file/folder/name] [output]");
			System.exit(-1);
		}

		String infile = args[0];
		String outfile = args[1];

		sLogger.info("input: " + infile);
		sLogger.info("output: " + outfile);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FileStatus input = fs.getFileStatus(new Path(infile));
		
		
		DataOutputStream os = new DataOutputStream(fs.create(new Path(outfile)));
		BufferedWriter data = new BufferedWriter(new OutputStreamWriter(os));
		
		
		long cnt = 0;
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
      // read joined sequential file, line separated

      Reader reader = new Reader(fs, file.getPath(), conf);
      Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
      
      
  		String line;
  		while(reader.next(key, value)) {
  			line = ((ArrayListWritable<Text>)((Tuple)key).get(0)).toString();
  			data.write(line + "\n");
  			cnt++;
  		}
  		
  		data.close();
  		reader.close();

      sLogger.info("Completed processing: " + file.getPath().toUri().getPath());
    }

    sLogger.info("Wrote " + cnt + " records.");
	}
}