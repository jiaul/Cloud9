package edu.umd.cloud9.ranking;

import java.util.Random;
import java.util.concurrent.RecursiveAction;
import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

import tl.lin.data.array.IntArrayWritable;

import org.clueweb.data.PForDocVector;

import edu.umd.cloud9.ranking.Score;

public class ForkJoinThread extends RecursiveAction {
  private int start, end, cutoff;
  private IntArrayWritable[] docvectors;
  private String[] keys;
  private int [] qtid;
  private float [] idf;
  private int qlen;
  private int cs;
  private int[] tf = new int[10];
  //private static final PForDocVector DOC = new PForDocVector();

    public ForkJoinThread(int start, int end, IntArrayWritable[] docvectors, int[] qtid, float[] idf, int qlen, int cs, String[] keys, int cutoff) {
      this.start = start;
      this.end = end;
      this.docvectors = docvectors;
      this.qtid = qtid;
      this.idf = idf;
      this.qlen = qlen;
      this.cs = cs;
      this.keys = keys;
      this.cutoff = cutoff;
    }

    protected void computeDirectly() {
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
      final PForDocVector DOC = new PForDocVector();
      System.out.println(end-start + "\t" + keys[0]);
      System.out.println(docvectors[0].toString());
       for(int i = start; i < start+1; i++) {
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

    @Override
    protected void compute() {
        if (end-start <= cutoff) {
            computeDirectly();
            return;
        }

        int split = (end-start) / 2;

        invokeAll(new ForkJoinThread(start, start+split, docvectors, qtid, idf, qlen, cs, keys, cutoff),
                new ForkJoinThread(start+split+1, end, docvectors, qtid, idf, qlen, cs, keys, cutoff));
    }

    public static void main(String[] args) throws Exception {
        int[] A = new int[100000];
        Random random = new Random();
        for(int i = 0; i < 100000; i++)
          A[i] = random.nextInt(100000);
        FindMax(A);
        
    }

    public static void FindMax(int[] A) {
        int[] src = A;
        int[] dst = new int[src.length];

        int processors = Runtime.getRuntime().availableProcessors();
        System.out.println(Integer.toString(processors) + " processor"
                + (processors != 1 ? "s are " : " is ")
                + "available");

       /* ForkJoinThread fb = new ForkJoinThread(src, 0, src.length, dst);

        ForkJoinPool pool = new ForkJoinPool();

        long startTime = System.currentTimeMillis();
        pool.invoke(fb);
        long endTime = System.currentTimeMillis(); 

        System.out.println("Processing took " + (endTime - startTime) + " milliseconds."); */
    }
}
