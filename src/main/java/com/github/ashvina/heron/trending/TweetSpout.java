package com.github.ashvina.heron.trending;

import java.util.Map;

import com.github.ashvina.common.Restriction;
import com.github.ashvina.common.TweetsGenerator;

import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Values;

import static com.github.ashvina.heron.trending.TopTrendingTopology.FIELD_TEXT;
import static com.github.ashvina.heron.trending.TopTrendingTopology.FIELD_TREND;

public class TweetSpout extends BaseRichSpout {
  private Restriction restriction;

  private SpoutOutputCollector collector;
  private TweetsGenerator tweetsGenerator;
  private String tweetsFilePath;

  TweetSpout(String tweetsFilePath) {
    this.tweetsFilePath = tweetsFilePath;
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    tweetsGenerator = new TweetsGenerator(tweetsFilePath);
    restriction = new Restriction(context.getThisTaskId(), context.getThisComponentId(), Restriction.getYarnContainerId());
    this.collector = collector;
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
