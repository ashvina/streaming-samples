package com.github.ashvina.heron.trending;

import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;

import com.github.ashvina.heron.BaseRateLimitedBolt;

import org.apache.storm.starter.tools.Rankings;

import com.twitter.heron.api.bolt.BasicOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;

import static com.github.ashvina.heron.trending.TopTrendingTopology.FIELD_RANKING;

public class TotalRanker extends BaseRateLimitedBolt {
  private Duration emitRate;
  private int topN;
  private long previousEmitTime;
  private Rankings rankings;

  TotalRanker(Duration emitRate, int topN) {
    this.emitRate = emitRate;
    this.topN = topN;
  }

  @Override
  public void prepare(Map<String, Object> map, TopologyContext context) {
    super.prepare(map, context);
    previousEmitTime = System.currentTimeMillis();
    rankings = new Rankings(topN);
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    restriction.execute();
    Rankings partialRank = (Rankings) tuple.getValueByField(FIELD_RANKING);
    rankings.updateWith(partialRank);

    if (System.currentTimeMillis() - previousEmitTime > emitRate.toMillis()) {
      String result = rankings.getRankings().stream()
          .map(trend -> trend.getObject() + ":" + trend.getCount())
          .collect(Collectors.joining(", "));
      System.out.println(result);
      previousEmitTime = System.currentTimeMillis();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELD_RANKING));
  }
}
