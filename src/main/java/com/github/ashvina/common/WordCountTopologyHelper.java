package com.github.ashvina.common;

import org.apache.commons.cli.*;

public class WordCountTopologyHelper {
  public static final String FIELD_WORD = "word";
  public static final String FIELD_SENTENCE = "sentence";

  public static int spouts;
  public static int wordBolts;
  public static int countBolts;
  public static int numWorkers;

  public WordCountTopologyHelper(String[] args) throws Exception {
    parseOptions(args);
  }

  private static void parseOptions(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(Option.builder("spouts")
        .desc("number of random sentence producing spouts")
        .hasArg()
        .required()
        .build());

    options.addOption(Option.builder("words")
        .desc("number of words producing bolts")
        .hasArg()
        .required()
        .build());

    options.addOption(Option.builder("counts")
        .desc("number of counting bolts")
        .hasArg()
        .required()
        .build());

    options.addOption(Option.builder("workers")
        .desc("number of workers")
        .hasArg()
        .required()
        .build());

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    spouts = getIntValue(cmd, "spouts");
    wordBolts = getIntValue(cmd, "words");
    countBolts = getIntValue(cmd, "counts");
    numWorkers = getIntValue(cmd, "workers");
  }

  private static int getIntValue(CommandLine cmd, String component) {
    int value = Integer.parseInt(cmd.getOptionValue(component));
    System.out.println(String.format("Component %s has %d instances", component, value));
    return value;
  }
}
