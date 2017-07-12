package com.github.ashvina.heron.trending;

import java.time.Duration;
import java.util.Map;

import com.github.ashvina.heron.BaseRateLimitedBolt;

import org.apache.storm.starter.tools.SlidingWindowCounter;

import com.twitter.heron.api.bolt.BasicOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;

import static com.github.ashvina.heron.trending.TopTrendingTopology.FIELD_COUNT;
import static com.github.ashvina.heron.trending.TopTrendingTopology.FIELD_TREND;

public class RollingCountBolt extends BaseRateLimitedBolt {
  private Duration window;
  private Duration emitRate;
  private SlidingWindowCounter<String> counter;
  private long previousEmitTime;

  RollingCountBolt(Duration window, Duration emitRate) {
    this.window = window;
    this.emitRate = emitRate;
    if (compteSlotCount() < 5) {
      throw new IllegalArgumentException("window should be longer than emit rate");
    }
  }

  @Override
  public void prepare(Map<String, Object> map, TopologyContext context) {
    super.prepare(map, context);
    counter = new SlidingWindowCounter<>(compteSlotCount());
    previousEmitTime = System.currentTimeMillis();
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    restriction.execute();
    String trend = tuple.getStringByField(FIELD_TREND);
    counter.incrementCount(trend);

    if (System.currentTimeMillis() - previousEmitTime > emitRate.toMillis()) {
      Map<String, Long> counts = counter.getCountsThenAdvanceWindow();
      counts.keySet().forEach(tag -> collector.emit(new Values(tag, counts.get(tag))));
      previousEmitTime = System.currentTimeMillis();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELD_TREND, FIELD_COUNT));
  }

  private int compteSlotCount() {
    return (int) (window.toMillis() / emitRate.toMillis());
  }
}
