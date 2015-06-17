package edu.umd.cloud9.ranking;

//import edu.umd.cloud9.ranking.Score;
//import edu.umd.cloud9.ranking.RankerThread;



import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.clueweb.data.PForDocVector;
import java.io.*;

import tl.lin.data.array.IntArrayWritable;

public class PrintDecomDocvector {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
  private PrintDecomDocvector() {}
  public static  DecomKeyValue[] data = new DecomKeyValue[1000000000];
  public static int numDocRead = 0;
  public static int numFile = 0;
  private static final PForDocVector DOC = new PForDocVector();
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.out.println("args: [path] [max-num-of-records-per-file] [output file name]");
      System.exit(-1);
    }

    String f = args[0];
    int numDoc;
    int max = Integer.parseInt(args[1]);
    FileSystem fs = FileSystem.get(new Configuration());
    Path p = new Path(f);
    if (fs.getFileStatus(p).isDir()) {
      numDoc = readSequenceFilesInDir(p, fs, max);
    } else {
      numDoc = readSequenceFile(p, fs, max);
    }
    PrintWriter out = new PrintWriter(new FileWriter(args[2])); 
    // print decompressed vectors 
    for(int i = 0; i < numDoc; i++) {
      out.print(data[i].key + " " + data[i].doc.length);
      for(int j = 0; j < data[i].doc.length; j++)
        out.print(" " + data[i].doc[j]);
      out.print("\n");
    }
    out.close();
  }
  
  

  private static int readSequenceFile(Path path, FileSystem fs, int max) throws IOException {
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, fs.getConf());
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
        PForDocVector.fromIntArrayWritable(value, DOC);
        data[numDocRead] = new DecomKeyValue(key.toString(), DOC.getTermIds());
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

