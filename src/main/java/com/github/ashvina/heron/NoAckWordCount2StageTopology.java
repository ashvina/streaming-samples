package com.github.ashvina.heron;

import com.github.ashvina.common.RandomSentenceGenerator;
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

import java.util.Map;

public class NoAckWordCount2StageTopology {
  public static void main(String[] args) throws Exception {
    WordCountTopologyHelper helper = new WordCountTopologyHelper(args);

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout", new AckingRandomWordSpout(), helper.spouts);
    builder.setBolt("count", new WordCount(), helper.countBolts)
        .fieldsGrouping("spout", new Fields(WordCountTopologyHelper.FIELD_WORD));

    Config conf = new Config();
    conf.setDebug(false);
    conf.setEnableAcking(false);
    conf.setNumStmgrs(helper.numWorkers);
    HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
  }

  public static class AckingRandomWordSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private RandomSentenceGenerator sentenceGenerator;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      sentenceGenerator = new RandomSentenceGenerator();
      this.collector = collector;
    }

    @Override
    public void nextTuple() {
      String sentence = sentenceGenerator.nextWord();
      collector.emit(new Values(sentence));
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
