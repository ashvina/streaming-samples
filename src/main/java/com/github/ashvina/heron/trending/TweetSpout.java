package com.github.ashvina.heron.trending;

import java.nio.file.Paths;
import java.util.Map;

import com.github.ashvina.common.Restriction;
import com.github.ashvina.common.TweetsGenerator;
import com.github.ashvina.heron.BaseRateLimitedSpout;

import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Values;

import static com.github.ashvina.heron.trending.TopTrendingTopology.FIELD_TEXT;
import static com.github.ashvina.heron.trending.TopTrendingTopology.FIELD_TREND;

public class TweetSpout extends BaseRateLimitedSpout {
  private SpoutOutputCollector collector;
  private TweetsGenerator tweetsGenerator;
  private String tweetsFilePath;

  TweetSpout(String tweetsFilePath) {
    this.tweetsFilePath = tweetsFilePath;
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    super.open(conf, context, collector);
    this.collector = collector;
    this.tweetsGenerator = new TweetsGenerator(tweetsFilePath);
  }

  @Override
  public void nextTuple() {
    TweetsGenerator.Tweet tweet = tweetsGenerator.nextTweet();
    while (tweet.getHashtags().isEmpty() && tweet.getMentions().isEmpty()) {
      tweet = tweetsGenerator.nextTweet();
    }

    String text = tweet.getText();
    tweet.getHashtags().forEach(tag -> {
      collector.emit(new Values(tag, text));
      restriction.execute();
    });
    tweet.getMentions().forEach(tag -> {
      collector.emit(new Values(tag, text));
      restriction.execute();
    });
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELD_TREND, FIELD_TEXT));
  }
}
