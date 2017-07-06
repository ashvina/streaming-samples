package com.github.ashvina.storm;

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

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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
    conf.setMaxSpoutPending(10000);
    conf.setNumWorkers(parser.getNumWorkers());
    StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
  }

  public static class AckingRandomWordSpout extends BaseRichSpout {
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
      String sentence = sentenceGenerator.nextWord(0);
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
