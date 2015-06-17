package edu.umd.cloud9.ranking;

import java.io.IOException;

//import edu.umd.cloud9.ranking.Score;
//import edu.umd.cloud9.ranking.RankerThread;




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

import edu.umd.cloud9.util.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;


public class BruteForceScanCompressed {

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
  private BruteForceScanCompressed() {}
  public static String[] keys = new String[100000000];
  public static IntArrayWritable[] docvectors = new IntArrayWritable[100000000];
  public static int numDocRead = 0;
  public static int numFile = 0;
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.out.println("args: [path] [# top documents] [dictionary path] [# thread or cutoff (for recursive forkjoin)]");
      System.exit(-1);
    }

    String f = args[0];
    String queryFile = "web-13-14.topics";
    // load dictionary
    String dictPath = args[2];
    Configuration conf = new Configuration();
    FileSystem fs1 = FileSystem.get(conf); 
    DefaultFrequencySortedDictionary dictionary = new DefaultFrequencySortedDictionary(dictPath, fs1);
    TermStatistics stats = new TermStatistics(new Path(dictPath), fs1);
    // read query and convert to ids
    queryTermtoID allQuery = new queryTermtoID(queryFile, dictionary);
    System.out.println("Number of query read: " + allQuery.nq);
   
    //int nTerms = dictionary.size();
    //System.out.println("Number of terms: " + nTerms);
    int [] qTermId = new int[100];
    int [] df = new int[100];
    float [] idf =  new float[100];
    int qlen;
    //sample query : barack obama famili
    String query = "barack obama famili";
    qlen = 0;
    for(String t : query.split(" ")) {
       qTermId[qlen] = dictionary.getId(t);
       df[qlen] = stats.getDf(qTermId[qlen]);
       qlen++;
    } 
    for(int j = 0; j < qlen; j++) {
      idf[j] = (float) Math.log((1.0f * 52000000)/df[j]);
    }
    
    int numDoc;
    int max = Integer.MAX_VALUE;

    FileSystem fs = FileSystem.get(new Configuration());
    Path p = new Path(f);

    if (fs.getFileStatus(p).isDirectory()) {
      numDoc = readSequenceFilesInDir(p, fs, max);
    } else {
      numDoc = readSequenceFile(p, fs, max);
    }
    
// compute ctf and idfs of query terms
    /*for(int k = 0; k < allQuery.nq; k++) {
      List<Float> idf = new ArrayList<Float>();
      List<Long> ctf = new ArrayList<Long>();
      for(int l = 0; l < allQuery.query[k].TermID.size(); l++) {
        int id = allQuery.query[k].TermID.get(l);
        int df = stats.getDf(id);
        idf.add((float) Math.log((1.0f*numDoc-df+0.5f)/(df+0.5f)));
        ctf.add(stats.getCf(id));
      }
      allQuery.query[k] = new Query(allQuery.query[k].qno, allQuery.query[k].TermID, idf, ctf);
    }*/
    
    // recursive forkjoin multithreading
      int cutoff = Integer.parseInt(args[3]);
      long startTime = System.currentTimeMillis();
      ForkJoinThread fb = new ForkJoinThread(0, numDoc, docvectors, qTermId, idf, qlen, numDoc, keys, cutoff);
      ForkJoinPool pool = new ForkJoinPool();
      pool.invoke(fb);
      long endTime = System.currentTimeMillis();
      System.out.print("Time " + (endTime-startTime) + " ms " + "Doc processed: " + numDoc + " cutoff " + cutoff);
      System.out.println(" Thread pool size: " + pool.getPoolSize());
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
        docvectors[numDocRead] = value;
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
