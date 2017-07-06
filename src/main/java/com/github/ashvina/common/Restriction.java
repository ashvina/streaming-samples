package com.github.ashvina.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.yaml.snakeyaml.Yaml;

public class Restriction {
  private String taskId;
  private String componentId;
  private String containerId;

  private long startTimeMillis = 0;
  private double MINUTE = Duration.ofMinutes(1).toMillis();
  private long previousSetRateTime = 0;
  private int windowSizeMillis = 20;
  private double maxTuplesPerWindow = 0;
  private int executeCount = 0;

  private int skewPercent = 0;

  public Restriction(String taskId, String componentId, String containerId) {
    this.taskId = taskId;
    this.componentId = componentId;
    this.containerId = containerId;
    System.out.println(String.format("Task:%s, component:%s, container:%s", taskId, componentId, containerId));

    startTimeMillis = System.currentTimeMillis();
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

    double tpm = 0;
    maxTuplesPerWindow = 0;
    try {
      Yaml yaml = new Yaml();
      Map<String, Object> delayMap =
          (Map<String, Object>) yaml.load(new FileInputStream("/tmp/heron.yaml"));
      Map<String, String> taskConfig = (Map<String, String>) delayMap.get(taskId + "");
      Map<String, String> componentConfig = (Map<String, String>) delayMap.get(componentId);

      if (taskConfig != null) {
        String containerToBeDelayed = taskConfig.get("container");
        if (containerToBeDelayed == null || containerId.endsWith(containerToBeDelayed)) {
          tpm = Double.valueOf(taskConfig.get("tpm"));
        }
      } else if (componentConfig != null) {
        tpm = Double.valueOf(componentConfig.get("tpm"));
        if (componentConfig.containsKey("skew")) {
          this.skewPercent = Integer.valueOf(componentConfig.get("skew"));
        }
      }

      if (tpm > 0) {
        maxTuplesPerWindow = (tpm * windowSizeMillis) / MINUTE;
      }

      System.out.println(String.format("TPM for %s:%s in %s is %f, i.e. %.2f in %d millis",
          componentId, taskId, containerId, tpm, maxTuplesPerWindow, windowSizeMillis));
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
