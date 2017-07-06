package com.github.ashvina.heron;

import com.github.ashvina.common.RandomSentenceGenerator;
import com.github.ashvina.common.Restriction;
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

public class NoAckWordCountTopology {
  public static void main(String[] args) throws Exception {
    WordCountTopologyHelper helper = new WordCountTopologyHelper(args);
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout", new NoAckStormRandomSentenceSpout(), helper.spouts);
    builder.setBolt("split", new SplitSentence(), helper.wordBolts).shuffleGrouping("spout");
    builder.setBolt("count", new WordCount(), helper.countBolts)
        .fieldsGrouping("split", new Fields(WordCountTopologyHelper.FIELD_WORD));

    Config conf = new Config();
    conf.setDebug(false);
    conf.setEnableAcking(false);
    conf.setNumStmgrs(helper.numWorkers);
    HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
  }

  public static class NoAckStormRandomSentenceSpout extends BaseRichSpout {
    public static final int SENTENCE_SIZE = 200;
    Restriction restriction;

    private SpoutOutputCollector collector;
    private RandomSentenceGenerator sentenceGenerator;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      sentenceGenerator = new RandomSentenceGenerator();
      this.collector = collector;
      restriction = new Restriction(context.getThisTaskId() + "", context.getThisComponentId(), Restriction.getYarnContainerId());
    }

    @Override
    public void nextTuple() {
      restriction.execute();
      String sentence = sentenceGenerator.nextSentence(SENTENCE_SIZE, restriction.getSkewPercent());
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
      declarer.declare(new Fields(WordCountTopologyHelper.FIELD_SENTENCE));
    }
  }
}
