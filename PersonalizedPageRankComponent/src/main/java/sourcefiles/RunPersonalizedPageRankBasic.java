package sourcefiles;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import edu.umd.cloud9.mapreduce.lib.input.NonSplitableSequenceFileInputFormat;
import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.array.ArrayListWritable;

public class RunPersonalizedPageRankBasic extends Configured implements Tool {

  private static Logger LOG = Logger
      .getLogger(RunPersonalizedPageRankBasic.class);
  private static final String NUMBER_OF_SOURCES = "numberOfSources";
  private static final String SOURCE_VALUES = "sourceValues";

  private static enum PageRank {
    nodes, edges, massMessages, massMessagesSaved, massMessagesReceived, missingStructure
  };

  /*
   * MAPPER I - No in mapper combining
   */
  private static class MapClass
      extends
      Mapper<Text, PageRankNodeEnhanced, Text, PageRankNodeEnhanced> {
    // contains the node id
    private static final Text neighbor = new Text();
    // contains the intermediate mass
    private static final PageRankNodeEnhanced intermediateMass = new PageRankNodeEnhanced();
    // contains the structure
    private static final PageRankNodeEnhanced intermediateStructure = new PageRankNodeEnhanced();
    int numberOfSources;

    @Override
    public void setup(Context context) {
      numberOfSources = context.getConfiguration().getInt(NUMBER_OF_SOURCES, 0);
      if (numberOfSources == 0) {
        throw new RuntimeException(NUMBER_OF_SOURCES
            + " cannot be 0!. No sources provided");
      }
    }

    @Override
    public void map(Text nid, PageRankNodeEnhanced node, Context context)
        throws IOException, InterruptedException {
      // First pass the node structure
      intermediateStructure.setNodeId(node.getNodeId());
      intermediateStructure.setType(PageRankNodeEnhanced.Type.Structure);
      intermediateStructure.setAdjacencyList(node.getAdjacenyList());

      context.write(nid, intermediateStructure);// Write it the context

      // Now we distribute the page rank across to the neighbors
      int massMessages = 0;
      if (node.getAdjacenyList().size() > 0) {
        ArrayListWritable<Text> list = node.getAdjacenyList();

        // Writing the array of floats
        float pagerankArray[] = new float[numberOfSources];
        for (int i = 0; i < numberOfSources; i++) {
          pagerankArray[i] =
              node.getPageRankList().get(i) - (float) StrictMath.log(list.size());
        }

        ArrayListOfFloatsWritable pageranks =
            new ArrayListOfFloatsWritable(pagerankArray);

        context.getCounter(PageRank.edges).increment(list.size());

        // iterate over the neighbors for sending the above calculated page
        // rank
        for (int i = 0; i < list.size(); i++) {
          neighbor.set(list.get(i));
          intermediateMass.setNodeId(list.get(i).toString());
          intermediateMass.setType(PageRankNodeEnhanced.Type.Mass);
          intermediateMass.setPageRankList(pageranks);

          // now write to the context
          context.write(neighbor, intermediateMass);
          massMessages++;
        }
      }

      // Bookkeeping for saving the number of nodes and mass Messages
      context.getCounter(PageRank.nodes).increment(1);
      context.getCounter(PageRank.massMessages).increment(massMessages);
    }
  }

  /*
   * COMBINER I
   */
  private static class CombineClass extends
      Reducer<Text, PageRankNodeEnhanced, Text, PageRankNodeEnhanced> {

    private static final PageRankNodeEnhanced intermediateMass = new PageRankNodeEnhanced();
    int numberOfSources = 0;
    
    @Override
    public void setup(Context context) {
      numberOfSources = context.getConfiguration().getInt(NUMBER_OF_SOURCES, 0);
      if (numberOfSources == 0) {
        throw new RuntimeException(NUMBER_OF_SOURCES + " cannot be 0!. No sources provided");
      }
    }
    
    @Override
    public void reduce(Text nid, Iterable<PageRankNodeEnhanced> values,
        Context context) throws IOException, InterruptedException {
      
      int massMessages = 0;

      float pagerankMassArray[] = new float[numberOfSources];
      for (int i = 0; i < pagerankMassArray.length; i++) {
        pagerankMassArray[i] = Float.NEGATIVE_INFINITY;
      }
      for (PageRankNodeEnhanced n : values) {
        // now check if the node is of type structure or not
        if (n.getType() == PageRankNodeEnhanced.Type.Structure) {
          // simply pass the structure to context
          context.write(nid, n);
        } else {
          // accumulate the mass contributions
          for (int j=0;j<pagerankMassArray.length;j++) {
            pagerankMassArray[j] =
                sumLogProbs(pagerankMassArray[j], n.getPageRankList().get(j));
          }
          massMessages++;
        }
      }
      // emit the aggregated results
      if (massMessages > 0) { // means if it has at least one edge
        intermediateMass.setNodeId(nid.toString());
        intermediateMass.setType(PageRankNodeEnhanced.Type.Mass);
        ArrayListOfFloatsWritable accumulatedMasses = new ArrayListOfFloatsWritable(pagerankMassArray);
        intermediateMass.setPageRankList(accumulatedMasses);
        context.write(nid, intermediateMass);
      }
    }
  }

  /*
   * REDUCER I
   */
  private static class ReduceClass extends
      Reducer<Text, PageRankNodeEnhanced, Text, PageRankNodeEnhanced> {

    private float[] totalMassArray;
    int numberOfSources;

    @Override
    public void setup(Context context) {
      numberOfSources = context.getConfiguration().getInt(NUMBER_OF_SOURCES, 0);
      if (numberOfSources == 0) {
        throw new RuntimeException(NUMBER_OF_SOURCES
            + " cannot be 0!. No sources provided");
      }
      totalMassArray = new float[numberOfSources];
      for (int i = 0; i < numberOfSources; i++) {
        totalMassArray[i] = Float.NEGATIVE_INFINITY;
      }
    }

    @Override
    public void reduce(Text nid, Iterable<PageRankNodeEnhanced> iterable,
        Context context) throws IOException, InterruptedException {

      Iterator<PageRankNodeEnhanced> values = iterable.iterator();
      PageRankNodeEnhanced node = new PageRankNodeEnhanced();
      node.setType(PageRankNodeEnhanced.Type.Complete);
      node.setNodeId(nid.toString());

      int massMessagesReceived = 0;
      int structureReceived = 0;

      float pagerankMassArray[] = new float[numberOfSources];
      for (int i = 0; i < pagerankMassArray.length; i++) {
        pagerankMassArray[i] = Float.NEGATIVE_INFINITY;
      }
      while (values.hasNext()) {
        PageRankNodeEnhanced n = values.next();

        if (n.getType().equals(PageRankNodeEnhanced.Type.Structure)) {
          ArrayListWritable<Text> list = n.getAdjacenyList();
          structureReceived++;
          node.setAdjacencyList(list);
        }

        else {
          // This is a message that contains PageRank mass; accumulate.
          for (int j = 0; j < pagerankMassArray.length; j++) {
            pagerankMassArray[j] =
                sumLogProbs(pagerankMassArray[j], n.getPageRankList().get(j));
          }
          massMessagesReceived++;
        }

      }

      // update the final accumulated pagerank mass for the next iteration
      ArrayListOfFloatsWritable accumulatedMasses =
          new ArrayListOfFloatsWritable(pagerankMassArray);
      node.setPageRankList(accumulatedMasses);
      if (nid.toString().equals("")) {
        System.out.println("In reducer 1, PR is : "
            + accumulatedMasses.toString());
      }
      context.getCounter(PageRank.massMessagesReceived).increment(
          massMessagesReceived);

      // check error,
      if (structureReceived == 1) {
        // Everything checks out, emit final node structure with updated
        // PageRank value.
        context.write(nid, node);
        // accumulate the total mass
        for (int i = 0; i < totalMassArray.length; i++) {
          totalMassArray[i] =
              sumLogProbs(totalMassArray[i], accumulatedMasses.get(i));
        }
      } else if (structureReceived == 0) {
        // We get into this situation if there exists an edge pointing to a node
        // which has no
        // corresponding node structure (i.e., PageRank mass was passed to a
        // non-existent node)...
        // log and count but move on.
        context.getCounter(PageRank.missingStructure).increment(1);
        LOG.warn("No structure received for nodeid: " + nid.toString()
            + " mass: "
            + massMessagesReceived);

        // It's important to note that we don't add the PageRank mass to
        // total... if PageRank mass
        // was sent to a non-existent node, it should simply vanish.
      } else {
        // This shouldn't happen!
        throw new RuntimeException("Multiple structure received for nodeid: "
            + nid.toString() + " mass: " + massMessagesReceived + " struct: "
            + structureReceived);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      String taskId = conf.get("mapred.task.id");
      String path = conf.get("PageRankMassPath");

      Preconditions.checkNotNull(taskId);
      Preconditions.checkNotNull(path);

      // write to a file the page rank mass of the reducer
      ArrayListOfFloatsWritable totalMassList =
          new ArrayListOfFloatsWritable(totalMassArray);
      FileSystem fs = FileSystem.get(context.getConfiguration());
      FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);
      for (int i = 0; i < numberOfSources; i++) {
        out.writeFloat(totalMassList.get(i));
      }
      out.close();
    }
  }

  /*
   * MAPPER II
   */
  private static class MapPageRankMassDistributionClass extends
      Mapper<Text, PageRankNodeEnhanced, Text, PageRankNodeEnhanced> {

    private int numberOfSources = 0;
    private ArrayListOfFloatsWritable missingMassArrayList = null;
    private String[] sourcesArray = null;

    @Override
    public void setup(Context context) {
      numberOfSources = context.getConfiguration().getInt(NUMBER_OF_SOURCES, 0);
      if (numberOfSources == 0) {
        throw new RuntimeException(NUMBER_OF_SOURCES
            + " cannot be 0!. No sources provided");
      }
      // Parsing the sources and putting in a HashMap, where value is the index
      String line[] = context.getConfiguration().get(SOURCE_VALUES).split(" ");
      sourcesArray = new String[numberOfSources];
      for (int r = 0; r < line.length; r++) {
        sourcesArray[r] = line[r];
      }

      float missingMassArray[] = new float[numberOfSources];
      String[] missingMasses =
          context.getConfiguration().getStrings("MissingMasses");
      for (int j = 0; j < numberOfSources; j++) {
        missingMassArray[j] = Float.parseFloat(missingMasses[j]);
      }
      missingMassArrayList = new ArrayListOfFloatsWritable(missingMassArray);
    }

    @Override
    public void map(Text nid, PageRankNodeEnhanced node, Context context)
        throws IOException, InterruptedException {
      /*
       * check if the incoming node is a source node add the 1. Random jump
       * factor and 2. Missing mass at the correct index in its page rank list
       * and Write
       */

      ArrayListOfFloatsWritable prlist = node.getPageRankList();

      for (int m = 0; m < numberOfSources; m++) {
        float p = node.getPageRankList().get(m);
        float jump = (float) Math.log(0.0f);
        float link = 0.0f;

        if (sourcesArray[m].equals(node.getNodeId())) {
          jump = (float) (Math.log(ALPHA));
          link = (float) (Math.log(1.0f - ALPHA)) + sumLogProbs(p,(float) (Math.log(missingMassArrayList.get(m))));
          System.out.println(" link : " + link);
        }

        else {
          jump = (float) Math.log(0.0f);
          link = (float) Math.log(1.0f - ALPHA) + p;
        }

        p = sumLogProbs(jump, link);
        prlist.set(m, p);
      }
      node.setPageRankList(prlist);

      context.write(nid, node);
    }
  }

  private static float ALPHA = 0.15f;
  private static NumberFormat formatter = new DecimalFormat("0000");

  public RunPersonalizedPageRankBasic() {}

  private static final String BASE = "base";
  private static final String NUM_NODES = "numNodes";
  private static final String START = "start";
  private static final String END = "end";
  private static final String SOURCES = "sources";

  @Override
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("base path").create(BASE));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("start iteration").create(START));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("end iteration").create(END));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of nodes").create(NUM_NODES));
    options.addOption(OptionBuilder.withArgName("sources").hasArg()
        .withDescription("source nodes").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(BASE) || !cmdline.hasOption(START)
        || !cmdline.hasOption(END) || !cmdline.hasOption(NUM_NODES)
        || !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String basePath = cmdline.getOptionValue(BASE);
    int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
    int s = Integer.parseInt(cmdline.getOptionValue(START));
    int e = Integer.parseInt(cmdline.getOptionValue(END));
    String sourceValues = cmdline.getOptionValue(SOURCES);
    String sources[] = sourceValues.split(",");

    LOG.info("Tool name: RunPageRank");
    LOG.info(" - base path: " + basePath);
    LOG.info(" - num nodes: " + n);
    LOG.info(" - start iteration: " + s);
    LOG.info(" - end iteration: " + e);
    StringBuilder displaySources = new StringBuilder();
    for (int i = 0; i < sources.length; i++) {
      displaySources.append(sources[i] + " ");
    }
    LOG.info(" - sources are : " + displaySources);
    int numberOfSources = sources.length;

    // Iterate over the page rank
    for (int i = s; i < e; i++) {
      iteratePageRank(i, i + 1, basePath, n, displaySources, numberOfSources);
    }

    return 0;
  }

  private void iteratePageRank(int i, int j, String basePath, int numNodes,
      StringBuilder sources, int numberOfSources) throws Exception {

    // Each iteration consists of two phases (two MapReduce jobs).

    // Job 1: distribute PageRank mass along outgoing edges.
    ArrayListOfFloatsWritable massArray = new ArrayListOfFloatsWritable();
    massArray = phase1(i, j, basePath, numNodes, sources, numberOfSources);

    System.out.println("Mass array : " + massArray.toString());

    // Find out how much PageRank mass got lost at the dangling nodes.
    for (int m = 0; m < numberOfSources; m++) {
      float missing_at_index_m =
          1.0f - (float) StrictMath.exp(massArray.get(m));
      massArray.set(m, missing_at_index_m);
    }

    // Job 2: distribute missing mass, take care of random jump factor.
    phase2(i, j, massArray, basePath, numNodes, sources, numberOfSources);

  }

  private ArrayListOfFloatsWritable phase1(int i, int j, String basePath,
      int numNodes, StringBuilder sources, int numberOfSources)
      throws Exception {

    Job job = Job.getInstance(getConf());
    job.setJobName("PageRank:Basic:iteration" + j + ":Phase1");
    job.setJarByClass(RunPersonalizedPageRankBasic.class);

    String in = basePath + "/iter" + formatter.format(i);
    String out = basePath + "/iter" + formatter.format(j) + "t";
    String outm = out + "-mass";

    // We need to actually count the number of part files to get the number of
    // partitions (because
    // the directory might contain _log).
    int numPartitions = 0;
    for (FileStatus s : FileSystem.get(getConf()).listStatus(new Path(in))) {
      if (s.getPath().getName().contains("part-"))
        numPartitions++;
    }

    LOG.info("PageRank: iteration " + j + ": Phase1");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);
    LOG.info(" - nodeCnt: " + numNodes);
    LOG.info("computed number of partitions: " + numPartitions);
    LOG.info(" - sources are : " + sources);

    int numReduceTasks = numPartitions;

    job.getConfiguration().setInt("NodeCount", numNodes);
    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    job.getConfiguration().set("PageRankMassPath", outm);
    job.getConfiguration().set(SOURCE_VALUES, new String(sources));
    job.getConfiguration().setInt(NUMBER_OF_SOURCES, numberOfSources);

    job.setNumReduceTasks(numReduceTasks);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PageRankNodeEnhanced.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PageRankNodeEnhanced.class);

    job.setMapperClass(MapClass.class);

    job.setCombinerClass(CombineClass.class);

    job.setReducerClass(ReduceClass.class);

    FileSystem.get(getConf()).delete(new Path(out), true);
    FileSystem.get(getConf()).delete(new Path(outm), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in "
        + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    // This would be returned
    ArrayListOfFloatsWritable pagerankMassArrayList =
        new ArrayListOfFloatsWritable();
    for (int k = 0; k < numberOfSources; k++) {
      pagerankMassArrayList.add(Float.NEGATIVE_INFINITY);
    }

    FileSystem fs = FileSystem.get(getConf());
    for (FileStatus f : fs.listStatus(new Path(outm))) {
      FSDataInputStream fin = fs.open(f.getPath());
      for (i = 0; i < numberOfSources; i++) {
        float currValue = fin.readFloat();
        float mass_at_index_i =
            sumLogProbs(pagerankMassArrayList.get(i), currValue);
        pagerankMassArrayList.set(i, mass_at_index_i);
      }
      fin.close();
    }
    return pagerankMassArrayList;
  }

  private void phase2(int i, int j,
      ArrayListOfFloatsWritable missingMassArrayList, String basePath,
      int numNodes, StringBuilder sources, int numberOfSources)
      throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName("PageRank:Basic:iteration" + j + ":Phase2");
    job.setJarByClass(RunPersonalizedPageRankBasic.class);

    LOG.info("missing PageRank mass: " + missingMassArrayList.toString());
    LOG.info("number of nodes: " + numNodes);

    String in = basePath + "/iter" + formatter.format(j) + "t";
    String out = basePath + "/iter" + formatter.format(j);

    LOG.info("PageRank: iteration " + j + ": Phase2");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);

    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution",
        false);
    job.getConfiguration().setBoolean(
        "mapred.reduce.tasks.speculative.execution", false);
    job.getConfiguration().set(SOURCE_VALUES, sources.toString());
    job.getConfiguration().setInt(NUMBER_OF_SOURCES, numberOfSources);
    job.getConfiguration().setInt("NodeCount", numNodes);

    String[] missingMasses = new String[numberOfSources];
    for (int m = 0; m < missingMassArrayList.size(); m++) {
      missingMasses[m] = Float.toString(missingMassArrayList.get(m));
    }
    job.getConfiguration().setStrings("MissingMasses", missingMasses);

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PageRankNodeEnhanced.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PageRankNodeEnhanced.class);

    job.setMapperClass(MapPageRankMassDistributionClass.class);

    FileSystem.get(getConf()).delete(new Path(out), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in "
        + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

  }

  public static void main(String args[]) throws Exception {
    ToolRunner.run(new RunPersonalizedPageRankBasic(), args);
  }

  private static float sumLogProbs(float a, float b) {
    if (a == Float.NEGATIVE_INFINITY) {
      return b;
    }
    if (b == Float.NEGATIVE_INFINITY) {
      return a;
    }
    if (a < b) {
      return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
    }
    return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
  }
}
