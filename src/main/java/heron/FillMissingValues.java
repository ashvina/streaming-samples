package heron;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class FillMissingValues {
  private static int WINDOW = 5;
  private static int hourOffset;

  Pattern p = Pattern.compile("__(.*)/container\\_(\\d+)\\_(.+)\\_(\\d+)(/.*)?");

  public static void main(String[] args) throws Exception {
    Stream<String> inStream;
    FillMissingValues filler = new FillMissingValues();
    WINDOW = Integer.parseInt(args[0]);
    String inputFile = args[1];
    hourOffset = 0; // to account for change in day

    if (args.length > 2) {
      hourOffset = Integer.parseInt(args[2]);
    }

    inStream = Files.lines(Paths.get(inputFile));
    Map<Metric, PriorityQueue<Reading>> comps = new HashMap<>();
    inStream.map(filler::parse).forEach(r -> addReading(comps, r));

    LocalTime earliestTimestamp = null;
    for (PriorityQueue<Reading> readings : comps.values()) {
      LocalTime t = readings.stream().map(r -> r.timestamp).min(Comparator.naturalOrder()).get();
      if (earliestTimestamp == null || earliestTimestamp.compareTo(t) > 0) {
        earliestTimestamp = t;
      }
    }

    earliestTimestamp = earliestTimestamp.plusSeconds(WINDOW);
    LocalTime maxWindowTimeStamp = null;
    while (!comps.isEmpty()) {
      for (Iterator<Metric> iter = comps.keySet().iterator(); iter.hasNext(); ) {
        Metric metric = iter.next();

        if (comps.get(metric).isEmpty()) {
          iter.remove();
          continue;
        }

        PriorityQueue<Reading> readings = comps.get(metric);
        Reading reading = readings.peek();
        if (reading.timestamp.compareTo(earliestTimestamp.plusSeconds(2)) < 0) { // some buffer
          System.out.println(readings.poll());
          if (maxWindowTimeStamp == null || maxWindowTimeStamp.compareTo(reading.timestamp) < 0) {
            maxWindowTimeStamp = reading.timestamp;
          }
        } else {
          System.out.println(new Reading(metric.comp, metric.instanceId, metric.metricName, "-1", earliestTimestamp));
        }
      }
      earliestTimestamp = maxWindowTimeStamp.plusSeconds(WINDOW);
    }
  }

  private static void addReading(Map<Metric, PriorityQueue<Reading>> comps, Reading reading) {
    PriorityQueue<Reading> readings = comps.computeIfAbsent
        (reading.metric, k -> new PriorityQueue<Reading>(Comparator.comparing(o -> o.timestamp)));
    readings.add(reading);
  }

  private Reading parse(String line) {
    String[] result = line.split(",");

    LocalTime time = LocalTime.parse(result[0]).plusHours(hourOffset);
    String comp = result[3];
    String instance = result[2];
    String metricName = result[4];
    String value = result[5];

    if (comp.trim().equals("stmgr")) {
      Matcher m = p.matcher(metricName);
      if (m.find()) {
        comp = m.group(3);
        instance = m.group(4);
        metricName = m.group(1);
        if (m.group(5) != null) {
          metricName += m.group(5);
        }
      }
    }

    return new Reading(comp, instance, metricName, value, time);
  }

  static class Reading {
    Metric metric;
    LocalTime timestamp;
    String value;

    Reading(String comp, String instance, String metricName, String value, LocalTime timestamp) {
      this.metric = new Metric(comp.trim(), instance.trim(), metricName.trim());
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
