package com.github.ashvina.heron;

import com.github.ashvina.common.WordCountTopologyHelper;
import com.twitter.heron.api.bolt.BaseBasicBolt;
import com.twitter.heron.api.bolt.BasicOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;

public class SplitSentence extends BaseBasicBolt {
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String sentence = tuple.getStringByField(WordCountTopologyHelper.FIELD_SENTENCE);
    String[] words = sentence.split(" ");
    for (String word : words) {
      if (word.trim().isEmpty()) {
        continue;
      }
      collector.emit(new Values(word));
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(WordCountTopologyHelper.FIELD_WORD));
  }
}
