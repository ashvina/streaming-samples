package com.github.ashvina.storm;

import com.github.ashvina.common.WordCountTopologyHelper;
import org.apache.storm.Config;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class WordCount extends BaseBasicBolt {
  AtomicLong counter = new AtomicLong();

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Config conf = new Config();
    return conf;
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    long count = counter.incrementAndGet();
    if (count % 1000000 == 0) {
      System.out.println(tuple.getStringByField(WordCountTopologyHelper.FIELD_WORD));
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
}