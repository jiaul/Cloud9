package edu.umd.cloud9.ranking;

import java.util.Random;
import java.util.concurrent.RecursiveAction;
import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;
import edu.umd.cloud9.ranking.Score;
import edu.umd.cloud9.util.*;

public class NewDecomForkJoinThread extends RecursiveAction {
  private int start, end;
  private DecomKeyValue[] data;
  private Query query;
  private int cutoff;
  private int qlen;
  private int numTopDoc;

    public NewDecomForkJoinThread(int start, int end, DecomKeyValue[] data, Query query, int cutoff, int numTopDoc) {
      this.start = start;
      this.end = end;
      this.data = data;
      this.query = query;
      this.cutoff = cutoff;
      this.numTopDoc = numTopDoc;
      qlen = query.TermID.size();
    }

    protected void computeDirectly() {
      float score;
      final int[] tf = new int[10];
      PriorityQueue<Score> scoreQueue = new PriorityQueue<Score>(numTopDoc, new Comparator<Score>() {
        public int compare(Score a, Score b) {
           if(a.score < b.score)
             return -1;
           else
             return 1;
        }
      });
      
       int n = 0;
       int dlen;
       for(int i = start; i < end; i++) {
         Arrays.fill(tf, 0);
         dlen = data[i].doc.length;
         for (int termid : data[i].doc) {
           for(int j = 0; j < qlen; j++)
             if(query.TermID.get(j) == termid)
               tf[j]++;
         }
         score = 0.0f;
         float k1 = 1.0f;
         float b = 0.5f;
         float adl = 450.0f;
         for(int k = 0; k < qlen; k++)
           score += query.idf.get(k) * ((k1+1.0f) * tf[k])/(k1*(1.0f-b+b*dlen/adl)+tf[k]);
         if(n < numTopDoc) {
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
       
       // print top k results
       int scoreQSize = Math.min(n, scoreQueue.size());
       for(int k = 0; k < scoreQSize; k++) {
         Score t = scoreQueue.poll();
         System.out.println(query.qno + "\t" + t.docid + "\t" + t.score);
       }
    }

    @Override
    protected void compute() {
        if (end-start <= cutoff) {
            computeDirectly();
            return;
        }

        int split = (end-start) / 2;

        invokeAll(new NewDecomForkJoinThread(start, start+split, data, query, cutoff, numTopDoc),
                new NewDecomForkJoinThread(start+split+1, end, data, query, cutoff, numTopDoc));
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


