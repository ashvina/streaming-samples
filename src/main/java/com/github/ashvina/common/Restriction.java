package com.github.ashvina.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.yaml.snakeyaml.Yaml;

import com.twitter.heron.api.topology.TopologyContext;

public class Restriction {
  private String taskId;
  private String componentId;
  private String containerId;
  private String configFilePath;

  private Duration configRefreshDuration = Duration.ofSeconds(5);
  private long previousSetRateTime = 0;
  private double rps;
  private long requestLatencyNano;
  private int requestsCount;
  private long realLatency;

  private Map<String, String> taskConfig;
  private Map<String, String> componentConfig;
  private double skewPercent = 0;

  public Restriction(TopologyContext context, String containerId, String fileName) {
    this(context.getThisTaskId() + "",
        context.getThisComponentId(),
        containerId,
        Paths.get("/tmp", fileName).toString());
  }

  private Restriction(String taskId, String componentId, String containerId, String file) {
    this.taskId = taskId;
    this.componentId = componentId;
    this.containerId = containerId;
    this.configFilePath = file;
    System.out.println(String.format("Task:%s, component:%s, container:%s", taskId, componentId, containerId));
  }

  public static String getYarnContainerId() {
    return Paths.get(new File(".").getAbsolutePath()).getParent().getFileName().toString();
  }

  public double getSkewPercent() {
    return skewPercent;
  }

  private void setConfigParams() {
    if (System.nanoTime() - previousSetRateTime < configRefreshDuration.toNanos()) {
      return;
    }
    previousSetRateTime = System.nanoTime();
    requestsCount = 0;
    realLatency = 0;

    try {
      Yaml yaml = new Yaml();
      Map<String, Object> delayMap =
          (Map<String, Object>) yaml.load(new FileInputStream(configFilePath));
      taskConfig = (Map<String, String>) delayMap.get(taskId + "");
      componentConfig = (Map<String, String>) delayMap.get(componentId);

      rps = getConfigValue("tpm") / 60;
      double amplitude = getConfigValue("amplitude") / 60;
      int lambdaSec = (int) getConfigValue("lambdaSec");
      this.skewPercent = getConfigValue("skew");

      if (rps > 0) {
        if (amplitude > 0 && lambdaSec > 0) {
          // sine curve for tpm variation
          double unitRadian = (2 * Math.PI) / lambdaSec;
          long window = Duration.ofMillis(System.currentTimeMillis()).getSeconds() % lambdaSec;
          rps += Math.sin(window * unitRadian) * amplitude;
        }
        requestLatencyNano = (long) (Duration.ofSeconds(1).toNanos() / rps);
      }

      System.out.println(String.format("Rate for %s:%s in %s = %.2f per sec, i.e. %d nano latency",
          componentId, taskId, containerId, rps, requestLatencyNano));

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
    long executeStart = System.nanoTime();
    setConfigParams();

    if (rps > 0) {
      if (requestsCount * requestLatencyNano < realLatency) {
        // we are too slow, don't throttle this request
      } else {
        // the sleep operation overhead could be as high as 2 millis.
        try {
          TimeUnit.NANOSECONDS.sleep(requestLatencyNano);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    requestsCount++;
    realLatency += System.nanoTime() - executeStart;
  }
}
