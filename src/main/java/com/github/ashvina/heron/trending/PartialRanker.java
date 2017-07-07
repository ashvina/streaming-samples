package com.github.ashvina.heron.trending;

import java.time.Duration;
import java.util.Map;

import com.github.ashvina.common.Restriction;

import org.apache.storm.starter.tools.RankableObjectWithFields;
import org.apache.storm.starter.tools.Rankings;

import com.twitter.heron.api.bolt.BaseBasicBolt;
import com.twitter.heron.api.bolt.BasicOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;

import static com.github.ashvina.heron.trending.TopTrendingTopology.FIELD_COUNT;
import static com.github.ashvina.heron.trending.TopTrendingTopology.FIELD_RANKING;
import static com.github.ashvina.heron.trending.TopTrendingTopology.FIELD_TREND;

public class PartialRanker extends BaseBasicBolt {
  private Restriction restriction;
  private Duration emitRate;
  private int topN;
  private long previousEmitTime;
  private Rankings rankings;

  PartialRanker(Duration emitRate, int topN) {
    this.emitRate = emitRate;
    this.topN = topN;
  }

  @Override
  public void prepare(Map<String, Object> map, TopologyContext context) {
    super.prepare(map, context);
    restriction = new Restriction(context.getThisTaskId(), context.getThisComponentId(), Restriction.getYarnContainerId());
    previousEmitTime = System.currentTimeMillis();
    rankings = new Rankings(topN);
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    restriction.execute();
    String trend = tuple.getStringByField(FIELD_TREND);
    Long count = tuple.getLongByField(FIELD_COUNT);

    RankableObjectWithFields rankable = new RankableObjectWithFields(trend, count, "");
    rankings.updateWith(rankable);

    if (System.currentTimeMillis() - previousEmitTime > emitRate.toMillis()) {
      collector.emit(new Values(rankings.copy()));
      previousEmitTime = System.currentTimeMillis();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELD_RANKING));
  }
}
