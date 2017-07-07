package com.github.ashvina.common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.twitter.Extractor;

/**
 * Reads a text file containing tweets and produces {@link Tweet} instances. Once end of file is
 * reached, it restarts serving from the first tweet.
 */
public class TweetsGenerator {
  private String filePath;
  private BufferedReader reader;

  public TweetsGenerator(String filePath) {
    this.filePath = filePath;
    try {
      prepareFileForReading();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private synchronized void prepareFileForReading() throws IOException {
    if (reader != null) {
      reader.close();
    }

    reader = new BufferedReader(new FileReader(filePath));
  }

  public Tweet nextTweet() {
    try {
      return _nextTweet();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Tweet _nextTweet() throws IOException {
    String line = reader.readLine();
    if (line == null) {
      prepareFileForReading();
      return _nextTweet();
    } else if (line.trim().isEmpty()) {
      return _nextTweet();
    }

    return new Tweet(line);
  }

  public static class Tweet {
    private final List<String> hashtags = new ArrayList<>();
    private final List<String> mentions = new ArrayList<>();
    private final String text;

    public Tweet(String text) {
      this.text = text;
      Extractor extractor = new Extractor();
      hashtags.addAll(extractor.extractHashtags(text));
      mentions.addAll(extractor.extractMentionedScreennames(text));
      String reply = extractor.extractReplyScreenname(text);
      if (reply != null && !reply.isEmpty()) {
        mentions.add(reply);
      }
    }

    public List<String> getHashtags() {
      return hashtags;
    }

    public List<String> getMentions() {
      return mentions;
    }

    public String getText() {
      return text;
    }

    @Override
    public String toString() {
      return "Tweet{" +
          "hashtags=" + hashtags +
          ", mentions=" + mentions +
          ", text='" + text + '\'' +
          '}';
    }
  }
}
