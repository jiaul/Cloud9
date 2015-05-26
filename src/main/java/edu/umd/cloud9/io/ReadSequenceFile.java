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

package edu.umd.cloud9.io;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import tl.lin.data.array.IntArrayWritable;

import org.clueweb.dictionary.*;
import org.clueweb.data.TermStatistics;

/**
 * <p>
 * Program that reads either a SequenceFile or a directory containing
 * SequenceFiles. A maximum number of key-value pairs to read must be specified;
 * in the of a directory, the value specifies the number of key-value pairs to
 * read <i>per file</i>.
 * </p>
 * 
 * <pre>
 * args: [path] [max-num-of-records] (local)
 * </pre>
 *
 * <p>
 * Note: specify "local" as the optional third argument for reading from local
 * disk.
 * </p>
 */
public class ReadSequenceFile {
  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
	private ReadSequenceFile() {}
	public static String[] keys = new String[100000000];
  public static IntArrayWritable[] values = new IntArrayWritable[100000000];
  public static int numDocRead = 0;
	public static void main(String[] args) throws IOException {
		if (args.length < 1) {
			System.out.println("args: [path] [max-num-of-records-per-file] [dictionary path] [number of thread]");
			System.exit(-1);
		}

		String f = args[0];
		
	  // load dictionary
		String dictPath = args[2];
		Configuration conf = new Configuration();
    FileSystem fs1 = FileSystem.get(conf); 
    DefaultFrequencySortedDictionary dictionary = new DefaultFrequencySortedDictionary(dictPath, fs1);
    TermStatistics stats = new TermStatistics(new Path(dictPath), fs1);
    
    int nTerms = dictionary.size();
    
    //System.out.println("Number of terms: " + nTerms);
    int [] qTermId = new int[100];
    int [] df = new int[100];
    float [] idf =  new float[100];
    int qlen;
    int numThread = Integer.parseInt(args[3]);
    
    //sample query : barack obama famili
    String query = "barack obama famili";
    qlen = 0;
    for(String t : query.split(" ")) {
       qTermId[qlen] = dictionary.getId(t);
       df[qlen] = stats.getDf(qTermId[qlen]);
       qlen++;
    }
    
		int i, numDoc;

		int max = Integer.MAX_VALUE;
		if (args.length >= 2) {
			max = Integer.parseInt(args[1]);
		}

		boolean useLocal = args.length >= 3 && args[2].equals("local") ? true : false;

		if (useLocal) {
			System.out.println("Reading from local filesystem");
		}

		FileSystem fs = useLocal? FileSystem.getLocal(new Configuration()) : FileSystem.get(new Configuration());
		Path p = new Path(f);

		if (fs.getFileStatus(p).isDir()) {
			numDoc = readSequenceFilesInDir(p, fs, max);
		} else {
			numDoc = readSequenceFile(p, fs, max);
		}
		
		for(int j = 0; j < qlen; j++) {
		  idf[j] = (float) Math.log((1.0f * numDoc)/df[j]);
		}
		
		/*for(i = 0; i < numDoc; i++) {
		  System.out.print(keys[i] + " ");
		  PForDocVector.fromIntArrayWritable(values[i], DOC);
		  for (int termid : DOC.getTermIds()) {
		    System.out.print(termid + " ");
      }
		  System.out.print("\n");
	  }*/
		
		
		
	}
	
	

	private static int readSequenceFile(Path path, FileSystem fs, int max) throws IOException {
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, fs.getConf());

		System.out.println("Reading " + path + "...\n");
		try {
			System.out.println("Key type: " + reader.getKeyClass().toString());
			System.out.println("Value type: " + reader.getValueClass().toString() + "\n");
		} catch (Exception e) {
			throw new RuntimeException("Error: loading key/value class");
		}

		Writable key;
		IntArrayWritable value;
		int n = 0;
		try {
      if ( Tuple.class.isAssignableFrom(reader.getKeyClass())) {
        key = TUPLE_FACTORY.newTuple();
      } else {
        key = (Writable) reader.getKeyClass().newInstance();
      }

      if ( Tuple.class.isAssignableFrom(reader.getValueClass())) {
        value = (IntArrayWritable) TUPLE_FACTORY.newTuple();
      } else {
        value = (IntArrayWritable) reader.getValueClass().newInstance();
      }

			while (reader.next(key, value)) {
			  //store docid and compressed docvectors in memory 
				values[numDocRead] = value;
				keys[numDocRead] = key.toString();
				numDocRead++;
				n++;

				if (n >= max)
					break;
			}
			reader.close();
			System.out.println(n + " records read.\n");
		} catch (Exception e) {
			e.printStackTrace();
		}

		return n;
	}

	private static int readSequenceFilesInDir(Path path, FileSystem fs, int max) {
		int n = 0;
		try {
			FileStatus[] stat = fs.listStatus(path);
			for (int i = 0; i < stat.length; ++i) {
				n += readSequenceFile(stat[i].getPath(), fs ,max);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println(n + " records read in total.");
		return n;
	}
}
