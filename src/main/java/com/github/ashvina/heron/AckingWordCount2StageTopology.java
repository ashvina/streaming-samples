package com.github.ashvina.heron;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.github.ashvina.common.RandomSentenceGenerator;
import com.github.ashvina.common.Restriction;
import com.github.ashvina.common.TopologyArgParser;
import com.github.ashvina.common.WordCountTopologyHelper;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Values;

import static com.github.ashvina.common.WordCountTopologyHelper.COUNT;
import static com.github.ashvina.common.WordCountTopologyHelper.SPOUT;

public class AckingWordCount2StageTopology {
  public static void main(String[] args) throws Exception {
    TopologyArgParser parser = new TopologyArgParser(args, SPOUT, COUNT);

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT, new AckingRandomWordSpout(), parser.get(SPOUT));
    builder.setBolt(COUNT, new WordCount(), parser.get(COUNT))
        .fieldsGrouping(SPOUT, new Fields(WordCountTopologyHelper.FIELD_WORD));

    Config conf = new Config();
    conf.setDebug(false);
    conf.setNumStmgrs(parser.getNumWorkers());
    conf.setEnableAcking(true);
    conf.setMaxSpoutPending(Integer.MAX_VALUE);
    HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
  }

  public static class AckingRandomWordSpout extends BaseRateLimitedSpout {
    AtomicLong messageId = new AtomicLong();
    private SpoutOutputCollector collector;
    private RandomSentenceGenerator sentenceGenerator;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      super.open(conf, context, collector);
      sentenceGenerator = new RandomSentenceGenerator();
      this.collector = collector;
    }

    @Override
    public void nextTuple() {
      restriction.execute();
      String sentence = sentenceGenerator.nextWord(restriction.getSkewPercent());
      collector.emit(new Values(sentence), messageId.incrementAndGet());
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(WordCountTopologyHelper.FIELD_WORD));
    }
  }
}
