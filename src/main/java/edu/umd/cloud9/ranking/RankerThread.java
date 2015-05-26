package edu.umd.cloud9.ranking;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

import tl.lin.data.array.IntArrayWritable;

import org.clueweb.data.PForDocVector;
import edu.umd.cloud9.ranking.Score;

public class RankerThread implements Runnable {
  private Thread t;
  private String threadName;
  private int start, end;
  private IntArrayWritable[] docvectors;
  private String[] keys;
  private int [] qtid;
  private float [] idf;
  private int qlen;
  private int cs;
  private int[] tf = new int[10];
  private static final PForDocVector DOC = new PForDocVector();
  
  public RankerThread(String name, int start, int end, IntArrayWritable[] docs, int[] qtid, float[] idf, int qlen, int cs, String[] keys){
      threadName = name;
      this.start = start;
      this.end = end;
      docvectors = docs;
      this.qtid = qtid;
      this.idf = idf;
      this.qlen = qlen;
      this.cs = cs;
      this.keys = keys;
      System.out.println("Creating " +  threadName );
  }
  
  public void run() {
    float score;
    PriorityQueue<Score> scoreQueue = new PriorityQueue<Score>(10, new Comparator<Score>() {
      public int compare(Score a, Score b) {
         if(a.score < b.score)
           return -1;
         else
           return 1;
      }
    });
    
    int n = 0;
     for(int i = start; i < end; i++) {
       PForDocVector.fromIntArrayWritable(docvectors[i], DOC);
       Arrays.fill(tf, 0);
       for (int termid : DOC.getTermIds()) {
         for(int j = 0; j < qlen; j++)
           if(qtid[j] == termid)
             tf[j]++;
       }
       score = 0.0f;
       for(int k = 0; k < qlen; k++)
         score += (1.0f * tf[k])/(1.0f + tf[k]) * idf[k];
       if(n < 10) {
         scoreQueue.add(new Score(keys[i], score));
         n++;
       }
       else {
         if(scoreQueue.peek().score < score) {
           scoreQueue.poll();
           scoreQueue.add(new Score(keys[i], score));
         }
       }
     }
     // print top 10 results
     for(int k = 0; k < 10; k++) {
       Score t = scoreQueue.poll();
       System.out.println(t.docid + "\t" + t.score);
     }
  }
  
  public void start ()
  {
     if (t == null)
     {
        t = new Thread (this, threadName);
        t.start ();
     }
  }



  public static void main(String args[]) {
  
     int[] myList = new int[10];
     for(int i = 0; i < 10; i++)
       myList[i] = i;
   /*  RankerThread R1 = new RankerThread( "Thread-1", 0, 5, myList);
     R1.start();
     
     RankerThread R2 = new RankerThread( "Thread-2", 5, 10, myList);
     R2.start(); */
  }   
}
