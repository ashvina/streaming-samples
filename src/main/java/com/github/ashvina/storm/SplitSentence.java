package com.github.ashvina.storm;

import com.github.ashvina.common.WordCountTopologyHelper;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

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
