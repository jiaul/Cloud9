package edu.umd.cloud9.ranking;

public class Score {

  public String docid;
  public float score;
  
  public Score(String docid, float score) 
  {
    this.docid = docid;
    this.score = score;
  }
}
