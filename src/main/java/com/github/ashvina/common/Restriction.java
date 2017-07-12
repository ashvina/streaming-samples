package com.github.ashvina.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.RateLimiter;

import org.yaml.snakeyaml.Yaml;

import com.twitter.heron.api.topology.TopologyContext;

public class Restriction {
  private String taskId;
  private String componentId;
  private String containerId;
  private String configFilePath;

  private long MINUTE = Duration.ofMinutes(1).getSeconds();
  private long previousSetRateTime = 0;

  private int skewPercent = 0;
  private RateLimiter limiter;
  private Map<String, String> taskConfig;
  private Map<String, String> componentConfig;

  public Restriction(TopologyContext context, String containerId, String fileName) {
    this(context.getThisTaskId(),
        context.getThisComponentId(),
        containerId,
        Paths.get("/tmp", fileName).toString());
  }

  public Restriction(int taskId, String componentId, String containerId, String file) {
    this(taskId + "", componentId, containerId, file);
  }

  public Restriction(String taskId, String componentId, String containerId, String file) {
    this.taskId = taskId;
    this.componentId = componentId;
    this.containerId = containerId;
    this.configFilePath = file;
    System.out.println(String.format("Task:%s, component:%s, container:%s", taskId, componentId, containerId));
  }

  public static String getYarnContainerId() {
    return Paths.get(new File(".").getAbsolutePath()).getParent().getFileName().toString();
  }

  public int getSkewPercent() {
    return skewPercent;
  }

  private void setConfigParams() {
    if (System.currentTimeMillis() - previousSetRateTime < TimeUnit.SECONDS.toMillis(5)) {
      return;
    }
    previousSetRateTime = System.currentTimeMillis();

    try {
      Yaml yaml = new Yaml();
      Map<String, Object> delayMap =
          (Map<String, Object>) yaml.load(new FileInputStream(configFilePath));
      taskConfig = (Map<String, String>) delayMap.get(taskId + "");
      componentConfig = (Map<String, String>) delayMap.get(componentId);

      int tpm = (int) getConfigValue("tpm");
      int amplitude = (int) getConfigValue("amplitude");
      int lambdaSec = (int) getConfigValue("lambdaSec");
      this.skewPercent = (int) getConfigValue("skew");

      long maxTuplesPerWindow = Integer.MAX_VALUE;
      if (tpm > 0) {
        maxTuplesPerWindow = tpm / MINUTE;

        if (amplitude > 0 && lambdaSec > 0) {
          // sine curve for tpm variation
          amplitude /= MINUTE;
          double unitRadian = (2 * Math.PI) / lambdaSec;
          long window = Duration.ofMillis(System.currentTimeMillis()).getSeconds() % lambdaSec;
          maxTuplesPerWindow += Math.sin(window * unitRadian) * amplitude;
        }

        limiter = RateLimiter.create(maxTuplesPerWindow);
      } else {
        limiter = null;
      }

      System.out.println(String.format("Current rate for %s:%s in %s is %d per sec",
          componentId, taskId, containerId, maxTuplesPerWindow));
    } catch (FileNotFoundException e) {
      System.out.println("Rate limiting config file not found: " + configFilePath);
    }
  }

  private double getConfigValue(String name) {
    double value = 0;
    if (taskConfig != null) {
      String containerId = taskConfig.get("container");
      if (containerId == null || this.containerId.endsWith(containerId)) {
        if (taskConfig.get(name) != null) {
          value = Double.parseDouble(taskConfig.get(name));
        }
      }
    } else if (componentConfig != null) {
      String containerId = componentConfig.get("container");
      if (containerId == null || this.containerId.endsWith(containerId)) {
        if (componentConfig.get(name) != null) {
          value = Double.parseDouble(componentConfig.get(name));
        }
      }
    }
    return value;
  }

  public void execute() {
    setConfigParams();
    if (limiter != null) {
      limiter.acquire();
    }
  }
}
