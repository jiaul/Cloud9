package edu.umd.cloud9.ranking;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;
import edu.umd.cloud9.ranking.Score;

public class DecomRankerThread implements Runnable {
  private Thread t;
  private String threadName;
  private int start, end;
  private int [] qtid;
  private float [] idf;
  private DecomKeyValue[] data;
  private int qlen;
  private int cs;
  private int[] tf = new int[10];
  
  public DecomRankerThread(String name, int start, int end, DecomKeyValue[] data, int[] qtid, float[] idf, int qlen, int cs){
      threadName = name;
      this.start = start;
      this.end = end;
      this.qtid = qtid;
      this.idf = idf;
      this.qlen = qlen;
      this.data = data;
      this.cs = cs;
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
   // System.out.println("Name: " +  threadName );
    int n = 0;
     for(int i = start; i < end; i++) {
       Arrays.fill(tf, 0);
       for (int termid : data[i].doc) {
         for(int j = 0; j < qlen; j++)
           if(qtid[j] == termid)
             tf[j]++;
       }
       score = 0.0f;
       if(score <= 0.0f)
         continue;
       for(int k = 0; k < qlen; k++)
         score += (1.0f * tf[k])/(1.0f + tf[k]) * idf[k];
       if(n < 10) {
         scoreQueue.add(new Score(data[i].key, score));
         n++;
       }
       else {
         if(scoreQueue.peek().score < score) {
           scoreQueue.poll();
           scoreQueue.add(new Score(data[i].key, score));
         }
       }
     }
     
     // print top 10 results
     for(int k = 0; k < Math.min(scoreQueue.size(), 10); k++) {
       Score t = scoreQueue.poll();
       System.out.println(t.docid + "\t" + t.score + "\t" + threadName);
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
   /*  DecomRankerThread R1 = new DecomRankerThread( "Thread-1", 0, 5, myList);
     R1.start();
     
     DecomRankerThread R2 = new DecomRankerThread( "Thread-2", 5, 10, myList);
     R2.start(); */
  }   
}
