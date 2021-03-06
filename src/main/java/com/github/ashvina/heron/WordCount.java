package com.github.ashvina.heron;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.github.ashvina.common.WordCountTopologyHelper;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.bolt.BasicOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.tuple.Tuple;

public class WordCount extends BaseRateLimitedBolt {
  AtomicLong counter = new AtomicLong();

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Config conf = new Config();
    return conf;
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    restriction.execute();
    long count = counter.incrementAndGet();
    if (count % 1000000 == 0) {
      System.out.println(tuple.getStringByField(WordCountTopologyHelper.FIELD_WORD));
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
}