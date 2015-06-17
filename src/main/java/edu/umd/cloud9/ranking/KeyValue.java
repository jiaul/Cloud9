package edu.umd.cloud9.ranking;

import tl.lin.data.array.IntArrayWritable;

public class KeyValue {
  public String[] keys;
  public IntArrayWritable[] docvectors;
  int length;
  
  public KeyValue()
  {
    keys = new String[1000000];
    docvectors = new IntArrayWritable[1000000];
    length = 0;
  }
}
