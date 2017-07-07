package com.github.ashvina.heron;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.github.ashvina.common.Restriction;
import com.github.ashvina.common.WordCountTopologyHelper;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.bolt.BaseBasicBolt;
import com.twitter.heron.api.bolt.BasicOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;

public class WordCount extends BaseBasicBolt {
  Restriction restriction;
  AtomicLong counter = new AtomicLong();

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Config conf = new Config();
    return conf;
  }

  @Override
  public void prepare(Map<String, Object> map, TopologyContext context) {
    super.prepare(map, context);
    restriction = new Restriction(context.getThisTaskId(), context.getThisComponentId(), Restriction.getYarnContainerId());
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