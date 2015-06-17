package edu.umd.cloud9.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.clueweb.data.TermStatistics;
import org.clueweb.dictionary.*;
import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/*import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;*/

public class termIDandStat {
 
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.out.println("args: [dictionary]");
      System.exit(-1);
    }
    String dictPath = args[0];
    Configuration conf = new Configuration();
    FileSystem fs1 = FileSystem.get(conf); 
    DefaultFrequencySortedDictionary dictionary = new DefaultFrequencySortedDictionary(dictPath, fs1);
    TermStatistics stats = new TermStatistics(new Path(dictPath), fs1);
    int size = dictionary.size();
    for(int i = 1; i <= size; i++) {
      System.out.println(i + "\t" + stats.getDf(i) + "\t" + stats.getCf(i));
    }
  }
}