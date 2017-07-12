package com.github.ashvina.heron;

import java.util.Map;

import com.github.ashvina.common.Restriction;

import com.twitter.heron.api.bolt.BaseBasicBolt;
import com.twitter.heron.api.topology.TopologyContext;

public abstract class BaseRateLimitedBolt extends BaseBasicBolt {
  protected Restriction restriction;

  @Override
  public void prepare(Map<String, Object> map, TopologyContext context) {
    String fileName = map.get("topology.name") + ".yaml";
    this.restriction = new Restriction(context, Restriction.getYarnContainerId(), fileName);
  }
}
