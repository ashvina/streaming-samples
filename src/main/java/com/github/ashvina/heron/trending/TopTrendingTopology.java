package com.github.ashvina.heron.trending;

import java.time.Duration;

import com.github.ashvina.common.TopologyArgParser;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.tuple.Fields;

public class TopTrendingTopology {

  public static final String SPOUT = "spout";
  private static final String COUNTER = "counter";
  private static final String PARTIAL = "partial";
  private static final String TOTAL = "total";

  static final String FIELD_TEXT = "text";
  static final String FIELD_TREND = "trend";
  static final String FIELD_COUNT = "count";
  static final String FIELD_RANKING = "ranking";

  public static void main(String[] args) throws Exception {
    TopologyArgParser parser = new TopologyArgParser(args, SPOUT, COUNTER, PARTIAL, TOTAL);

    TweetSpout tweetSpout = new TweetSpout("/tmp/tweets.txt");
    RollingCountBolt countBolt = new RollingCountBolt(Duration.ofSeconds(10), Duration.ofMillis(500));
    PartialRanker partialRanker = new PartialRanker(Duration.ofSeconds(5), 5);
    TotalRanker totalRanker = new TotalRanker(Duration.ofSeconds(5), 5);

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT, tweetSpout, parser.get(SPOUT));

    builder.setBolt(COUNTER, countBolt, parser.get(COUNTER))
        .fieldsGrouping(SPOUT, new Fields(FIELD_TREND));

    builder.setBolt(PARTIAL, partialRanker, parser.get(PARTIAL))
        .fieldsGrouping(COUNTER, new Fields(FIELD_TREND));

    builder.setBolt(TOTAL, totalRanker, parser.get(TOTAL))
        .globalGrouping(PARTIAL);


    Config conf = new Config();
    conf.setDebug(false);
    conf.setEnableAcking(false);
    conf.setNumStmgrs(parser.getNumWorkers());
    HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
  }
}
