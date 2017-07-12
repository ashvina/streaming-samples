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

public class Restriction {
  private String taskId;
  private String componentId;
  private String containerId;

  private long MINUTE = Duration.ofMinutes(1).getSeconds();
  private long previousSetRateTime = 0;

  private int skewPercent = 0;
  private RateLimiter limiter;

  public Restriction(int taskId, String componentId, String containerId) {
    this(taskId + "", componentId, containerId);
  }

  public Restriction(String taskId, String componentId, String containerId) {
    this.taskId = taskId;
    this.componentId = componentId;
    this.containerId = containerId;
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

    int tpm = 0;
    try {
      Yaml yaml = new Yaml();
      Map<String, Object> delayMap =
          (Map<String, Object>) yaml.load(new FileInputStream("/tmp/heron.yaml"));
      Map<String, String> taskConfig = (Map<String, String>) delayMap.get(taskId + "");
      Map<String, String> componentConfig = (Map<String, String>) delayMap.get(componentId);

      if (taskConfig != null) {
        String containerToBeDelayed = taskConfig.get("container");
        if (containerToBeDelayed == null || containerId.endsWith(containerToBeDelayed)) {
          tpm = Integer.parseInt(taskConfig.get("tpm"));
        }
      } else if (componentConfig != null) {
        tpm = Integer.parseInt(componentConfig.get("tpm"));
        if (componentConfig.containsKey("skew")) {
          this.skewPercent = Integer.valueOf(componentConfig.get("skew"));
        }
      }

      long maxTuplesPerWindow = Integer.MAX_VALUE;
      if (tpm > 0) {
        maxTuplesPerWindow = tpm / MINUTE;
        limiter = RateLimiter.create(maxTuplesPerWindow);
      } else {
        limiter = null;
      }

      System.out.println(String.format("TPM for %s:%s in %s is %d, i.e. %d per sec",
          componentId, taskId, containerId, tpm, maxTuplesPerWindow));


    } catch (FileNotFoundException e) {
      System.out.println("No delay config file found");
    }
  }

  public void execute() {
    setConfigParams();
    if (maxTuplesPerWindow > 0) {
      long currentTime = System.currentTimeMillis();
      int windowNumber = (int) ((currentTime - startTimeMillis) / windowSizeMillis) + 1;
      int maxExpectedCount = (int) (windowNumber * maxTuplesPerWindow);
      while (executeCount > maxExpectedCount) {
        long delay = windowNumber * windowSizeMillis - currentTime;
        delay = delay < 0 ? windowSizeMillis : delay;
        try {
          TimeUnit.MILLISECONDS.sleep(delay);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        currentTime = System.currentTimeMillis();
        windowNumber = (int) ((currentTime - startTimeMillis) / windowSizeMillis) + 1;
        maxExpectedCount = (int) (windowNumber * maxTuplesPerWindow);
      }
    }

    executeCount++;
  }
}
