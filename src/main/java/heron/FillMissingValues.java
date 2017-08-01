package heron;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Stream;

public class FillMissingValues {
  private static int WINDOW = 5;

  public static void main(String[] args) throws Exception {
    FillMissingValues filler = new FillMissingValues();
    WINDOW = Integer.parseInt(args[0]);
    String inputFile = args[1];

    Stream<String> inStream = Files.lines(Paths.get(inputFile));

    Map<Metric, PriorityQueue<Reading>> comps = new HashMap<>();
    inStream.map(filler::parse).forEach(r -> addReading(comps, r));

    LocalTime earliestTimestamp = null; // just make it futuristic
    for (PriorityQueue<Reading> readings : comps.values()) {
      LocalTime t = readings.stream().map(r -> r.timestamp).min(Comparator.naturalOrder()).get();
      if (earliestTimestamp == null) {
        earliestTimestamp = t;
      } else if (earliestTimestamp.compareTo(t) > 0) {
        earliestTimestamp = t;
      }
    }

    earliestTimestamp = earliestTimestamp.plusSeconds(WINDOW);
    while (!comps.isEmpty()) {
      for (Iterator<Metric> iter = comps.keySet().iterator(); iter.hasNext(); ) {
        Metric metric = iter.next();

        if (comps.get(metric).isEmpty()) {
          iter.remove();
          continue;
        }

        PriorityQueue<Reading> readings = comps.get(metric);
        Reading reading = readings.peek();
        if (reading.timestamp.compareTo(earliestTimestamp) < 0) {
          System.out.println(readings.poll());
        } else {
          System.out.println(new Reading(metric.comp, metric.instanceId, metric.metricName, "-1", earliestTimestamp));
        }
      }
      earliestTimestamp = earliestTimestamp.plusSeconds(WINDOW);
    }
  }

  private static void addReading(Map<Metric, PriorityQueue<Reading>> comps, Reading reading) {
    PriorityQueue<Reading> readings = comps.computeIfAbsent
        (reading.metric, k -> new PriorityQueue<Reading>(Comparator.comparing(o -> o.timestamp)));
    readings.add(reading);
  }

  private Reading parse(String line) {
    String[] result = line.split(",");

    LocalTime time = LocalTime.parse(result[0]);
    return new Reading(result[3], result[2], result[4], result[5], time);
  }

  static class Reading {
    Metric metric;
    LocalTime timestamp;
    String value;

    Reading(String comp, String instance, String metricName, String value, LocalTime timestamp) {
      this.metric = new Metric(comp.trim(), instance, metricName.trim());
      this.timestamp = timestamp;
      this.value = value.trim();
    }

    @Override
    public String toString() {
      return String.format("%s, %s, %s, %s, %s, %s",
          timestamp.toString(), "c",
          metric.instanceId,
          metric.comp,
          metric.metricName,
          value);
    }
  }

  static class Metric {
    String comp;
    String instanceId;
    String metricName;

    Metric(String comp, String instanceId, String metricName) {
      this.comp = comp;
      this.instanceId = instanceId;
      this.metricName = metricName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Metric that = (Metric) o;

      return comp.equals(that.comp) && instanceId.equals(that.instanceId) && metricName.equals(that.metricName);
    }

    @Override
    public int hashCode() {
      int result = comp.hashCode();
      result = 31 * result + instanceId.hashCode();
      result = 31 * result + metricName.hashCode();
      return result;
    }
  }
}
