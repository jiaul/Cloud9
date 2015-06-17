package edu.umd.cloud9.ranking;


import java.util.Random;
import java.util.concurrent.RecursiveAction;
import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;
import edu.umd.cloud9.ranking.Score;

public class DecomForkJoinThread extends RecursiveAction {
  private int start, end;
  private DecomKeyValue[] data;
  private int [] qtid;
  private float [] idf;
  private int qlen;
  private int cs;
  private int cutoff;

    public DecomForkJoinThread(int start, int end, DecomKeyValue[] data, int[] qtid, float[] idf, int qlen, int cs, int cutoff) {
      this.start = start;
      this.end = end;
      this.data = data;
      this.qtid = qtid;
      this.idf = idf;
      this.qlen = qlen;
      this.cs = cs;
      this.cutoff = cutoff;
    }

    protected void computeDirectly() {
      float score;
      final int[] tf = new int[10];
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
         Arrays.fill(tf, 0);
         for (int termid : data[i].doc) {
           for(int j = 0; j < qlen; j++)
             if(qtid[j] == termid)
               tf[j]++;
         }
         score = 0.0f;
         for(int k = 0; k < qlen; k++)
           score += (1.0f * tf[k])/(1.0f + tf[k]) * idf[k];
       //  if(score <= 0.0f)
         //  continue;
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
       for(int k = 0; k < Math.min(10, scoreQueue.size()); k++) {
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

        invokeAll(new DecomForkJoinThread(start, start+split, data, qtid, idf, qlen, cs, cutoff),
                new DecomForkJoinThread(start+split+1, end, data, qtid, idf, qlen, cs, cutoff));
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

       /* DecomForkJoinThread fb = new DecomForkJoinThread(src, 0, src.length, dst);

        ForkJoinPool pool = new ForkJoinPool();

        long startTime = System.currentTimeMillis();
        pool.invoke(fb);
        long endTime = System.currentTimeMillis(); 

        System.out.println("Processing took " + (endTime - startTime) + " milliseconds."); */
    }
}

