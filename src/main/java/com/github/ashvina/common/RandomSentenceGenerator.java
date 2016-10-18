package com.github.ashvina.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;

public class RandomSentenceGenerator {
  private Random rand;
  private ArrayList<String> wordList;

  public RandomSentenceGenerator() {
    rand = new Random();
    try {
      wordList = prepareWordList();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private ArrayList<String> prepareWordList() throws IOException {
    ArrayList<String> wordList = new ArrayList<>();
    BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/words.txt")));
    String line;
    while ((line = reader.readLine()) != null) {
      wordList.add(line + " ");
    }
    return wordList;
  }

  public String nextSentence(int desiredSentenceSize) {
    StringBuilder builder = new StringBuilder();
    while (desiredSentenceSize > 0) {
      String word = nextWord();
      desiredSentenceSize -= word.length();
      builder.append(word);
    }
    return builder.toString();
  }

  public String nextWord() {
    return wordList.get(rand.nextInt(wordList.size()));
  }
}
