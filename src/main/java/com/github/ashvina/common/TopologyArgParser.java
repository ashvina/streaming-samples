package com.github.ashvina.common;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class TopologyArgParser {
  private int numWorkers;
  private Map<String, Integer> compCounts = new HashMap<>();
  private List<String> compNames;

  public TopologyArgParser(String[] args, String... compNames) throws Exception {
    this.compNames = Arrays.asList(compNames);
    parseOptions(args);
  }

  public int get(String compName) {
    return compCounts.get(compName) != null ? compCounts.get(compName) : 1;
  }

  private void parseOptions(String[] args) throws Exception {
    Options options = new Options();

    compNames.stream().forEach(comp -> {
      options.addOption(Option.builder(comp)
          .desc("instance count for component: " + comp)
          .hasArg()
          .build());
    });

    options.addOption(Option.builder("workers")
        .desc("number of workers")
        .hasArg()
        .required()
        .build());

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    compNames.stream()
        .filter(comp -> cmd.hasOption(comp))
        .forEach(comp -> compCounts.put(comp, getIntValue(cmd, comp)));

    numWorkers = getIntValue(cmd, "workers");
  }

  private static int getIntValue(CommandLine cmd, String component) {
    int value = Integer.parseInt(cmd.getOptionValue(component));
    System.out.println(String.format("Component %s has %d instances", component, value));
    return value;
  }

  public int getNumWorkers() {
    return numWorkers;
  }
}
