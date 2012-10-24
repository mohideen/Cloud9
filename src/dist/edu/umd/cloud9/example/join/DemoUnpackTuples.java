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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
			System.out.println("usage: [input] [output]");
			System.exit(-1);
		}

		String infile = args[0];
		String outfile = args[1];

		sLogger.info("input: " + infile);
		sLogger.info("output: " + outfile);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Reader reader = new Reader(fs, new Path(infile), conf);
		Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		
		BufferedWriter data = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outfile)));
		
		long cnt = 0;
		
		String line;
		while(reader.next(key, value)) {
			line = ((ArrayListWritable<Text>)((Tuple)key).get(0)).toString();
			data.write(line + "\n");
			cnt++;
		}
		
		data.close();
		reader.close();
		
		sLogger.info("Wrote " + cnt + " records.");
	}
}