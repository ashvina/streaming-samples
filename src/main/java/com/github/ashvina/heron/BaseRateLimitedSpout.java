package com.github.ashvina.heron;

import java.util.Map;

import com.github.ashvina.common.Restriction;

import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.TopologyContext;

public abstract class BaseRateLimitedSpout extends BaseRichSpout {
  protected Restriction restriction;

  @Override
  public void open(Map<String, Object> map, TopologyContext context, SpoutOutputCollector collector) {
    String fileName = map.get("topology.name") + ".yaml";
    this.restriction = new Restriction(context, Restriction.getYarnContainerId(), fileName);
  }
}
