package com.github.ashvina.common;

public class WordCountTopologyHelper {
  public static final String FIELD_WORD = "word";
  public static final String FIELD_SENTENCE = "sentence";
  public static final String SPOUT = "spout";
  public static final String SPLIT = "split";
  public static final String COUNT = "count";

  public static int spouts;
  public static int wordBolts;
  public static int countBolts;
  public static int numWorkers;
}
