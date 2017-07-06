package com.github.ashvina.storm;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.github.ashvina.common.RandomSentenceGenerator;
import com.github.ashvina.common.TopologyArgParser;
import com.github.ashvina.common.WordCountTopologyHelper;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import static com.github.ashvina.common.WordCountTopologyHelper.COUNT;
import static com.github.ashvina.common.WordCountTopologyHelper.SPLIT;
import static com.github.ashvina.common.WordCountTopologyHelper.SPOUT;

public class AckingWordCountTopology {
  public static final int SENTENCE_SIZE = 200;

  public static void main(String[] args) throws Exception {
    TopologyArgParser parser = new TopologyArgParser(args, SPOUT, SPLIT, COUNT);


    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SPOUT, new AckingRandomSentenceSpout(), parser.get(SPOUT));
    builder.setBolt(SPLIT, new SplitSentence(), parser.get(SPLIT)).shuffleGrouping(SPOUT);
    builder.setBolt(COUNT, new WordCount(), parser.get(COUNT))
        .fieldsGrouping(SPLIT, new Fields(WordCountTopologyHelper.FIELD_WORD));

    Config conf = new Config();
    conf.setDebug(false);
    conf.setNumWorkers(parser.getNumWorkers());
    StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
  }

  public static class AckingRandomSentenceSpout extends BaseRichSpout {
    AtomicLong messageId = new AtomicLong();
    private SpoutOutputCollector collector;
    private RandomSentenceGenerator sentenceGenerator;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      sentenceGenerator = new RandomSentenceGenerator();
      this.collector = collector;
    }

    @Override
    public void nextTuple() {
      String sentence = sentenceGenerator.nextSentence(SENTENCE_SIZE);
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
      declarer.declare(new Fields(WordCountTopologyHelper.FIELD_SENTENCE));
    }
  }
}
