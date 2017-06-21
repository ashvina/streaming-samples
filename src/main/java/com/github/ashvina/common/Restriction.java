package com.github.ashvina.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.yaml.snakeyaml.Yaml;

import com.twitter.heron.api.topology.TopologyContext;

public class Restriction {
  private final String componentId;
  private int taskId;
  private String containerId;

  private long previousSetRateTime = 0;
  private static final int WINDOW_SIZE_MILLIS = 20;
  private int maxTuplesPerWindow = 0;
  private int tuplesSentSoFar = 0;
  private long windowStartTime = 0;

  public Restriction(TopologyContext context, String container) {
    this.taskId = context.getThisTaskId();
    this.componentId = context.getThisComponentId();
    containerId = container;
    System.out.println(String.format("Start task %d in container %s", taskId, containerId));
  }

  public static String getYarnContainerId() {
    return Paths.get(new File(".").getAbsolutePath()).getParent().getFileName().toString();
  }

  private void setConfigParams() {
    if (System.currentTimeMillis() - previousSetRateTime < 5000) {
      return;
    }
    previousSetRateTime = System.currentTimeMillis();

    int tpm = 0;
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
          tpm = Integer.valueOf(taskConfig.get("tpm"));
        }
      } else if (componentConfig != null) {
        tpm = Integer.valueOf(componentConfig.get("tpm"));
      }

      if (tpm > 0) {
        maxTuplesPerWindow = (tpm * WINDOW_SIZE_MILLIS) / (60 * 1000);
      }

      System.out.println(String.format("TPM for %s:%d in %s is %d, i.e. %d in %d millis",
          componentId, taskId, containerId, tpm, maxTuplesPerWindow, WINDOW_SIZE_MILLIS));
    } catch (FileNotFoundException e) {
      System.out.println("No delay config file found");
    }
  }

  public void execute() {
    setConfigParams();
    if (maxTuplesPerWindow > 0 && tuplesSentSoFar > maxTuplesPerWindow) {
      long delay = windowStartTime + WINDOW_SIZE_MILLIS - System.currentTimeMillis();
      delay = delay < 0 ? WINDOW_SIZE_MILLIS : delay;
      try {
        TimeUnit.MILLISECONDS.sleep(delay);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      windowStartTime = System.currentTimeMillis();
      tuplesSentSoFar = 0;
    }
    tuplesSentSoFar++;
  }
}
