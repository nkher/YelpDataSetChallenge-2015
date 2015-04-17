/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package sourcefiles;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.array.ArrayListWritable;

/**
 * <p>
 * Driver program that takes a plain-text encoding of a directed graph and builds corresponding
 * Hadoop structures for representing the graph.
 * </p>
 *
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class BuildPersonalizedPageRankRecords extends Configured implements Tool {
  
  private static final Logger LOG = Logger.getLogger(BuildPersonalizedPageRankRecords.class);

  private static final String NODE_CNT_FIELD = "node.cnt";
  private static final String NODE_SRC_FIELD = "node.src";

  private static class MyMapper extends
      Mapper<LongWritable, Text, Text, PageRankNodeEnhanced> {
    
    private static final Text nid = new Text();
    private static final PageRankNodeEnhanced node = new PageRankNodeEnhanced();
    private ArrayListOfFloatsWritable pageRankList = null;
    Text text = new Text();
    private String sources[];

    @Override
    public void setup(
        Mapper<LongWritable, Text, Text, PageRankNodeEnhanced>.Context context) {
    	
      sources = context.getConfiguration().get(NODE_SRC_FIELD).split(",");
      System.out.println("Source length:" + sources.length);
      
      if (sources.length == 0) {
        throw new RuntimeException("Sources have to be passed");
      }
      node.setType(PageRankNodeEnhanced.Type.Complete);
      node.setPageRankList(new ArrayListOfFloatsWritable(sources.length));
    }

    @Override
    public void map(LongWritable key, Text t, Context context)
        throws IOException, InterruptedException {
      String[] arr = t.toString().trim().split("\\s+");

      nid.set(arr[0]);
      if (arr.length == 1) {
        node.setNodeId(arr[0]);
        node.setAdjacencyList(new ArrayListWritable<Text>());

      } else {
        node.setNodeId(arr[0]);

        ArrayList<Text> neighbors = new ArrayList<Text>();
        for (int i = 1; i < arr.length; i++) {
          // text.set(arr[i]);
          neighbors.add(new Text(arr[i]));
        }
        ArrayListWritable<Text> al = new ArrayListWritable<Text>(neighbors);
        node.setAdjacencyList(al);
      }

      // pageRankList = new ArrayListOfFloatsWritable(sources.length);
      float f[] = new float[sources.length];
      
      for (int i = 0; i < sources.length; i++) 
      {
        if ((node.getNodeId()).equals(sources[i])) {
          f[i] = (float) StrictMath.log(1.0);
        } else {
          f[i] = (float) StrictMath.log(0.0);
        }
      }
      pageRankList = new ArrayListOfFloatsWritable(f);
      node.setPageRankList(pageRankList);
      
      context.getCounter("graph", "numNodes").increment(1);
      context.getCounter("graph", "numEdges").increment(arr.length - 1);

      if (arr.length > 1) {
        context.getCounter("graph", "numActiveNodes").increment(1);
      }

      context.write(nid, node);
    }
  }

  public BuildPersonalizedPageRankRecords() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_NODES = "numNodes";
  private static final String SOURCES = "sources";

  /**
   * Runs this tool.
   */
  @Override
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of nodes").create(NUM_NODES));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
    		.withDescription("sources").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(NUM_NODES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
    String sources = cmdline.getOptionValue(SOURCES);

    LOG.info("Tool name: " + BuildPersonalizedPageRankRecords.class.getSimpleName());
    LOG.info(" - inputDir: " + inputPath);
    LOG.info(" - outputDir: " + outputPath);
    LOG.info(" - numNodes: " + n);
    LOG.info(" - sources: " + sources);

    Configuration conf = getConf();
    conf.setInt(NODE_CNT_FIELD, n);
    conf.setStrings(NODE_SRC_FIELD, sources);
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);

    Job job = Job.getInstance(conf);
    job.setJobName(BuildPersonalizedPageRankRecords.class.getSimpleName() + ":" + inputPath);
    job.setJarByClass(BuildPersonalizedPageRankRecords.class);

    job.setNumReduceTasks(0);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    // job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PageRankNodeEnhanced.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PageRankNodeEnhanced.class);

    job.setMapperClass(MyMapper.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildPersonalizedPageRankRecords(), args);
  }
}
