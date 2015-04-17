package sourcefiles;

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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.pair.PairOfObjectFloat;
import tl.lin.data.queue.TopScoredObjects;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  
  private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);
  
  private static final String INPUT = "input";
  private static final String TOP = "top";
  private static final String SOURCES = "sources";
  
  @Override
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("top n").create(TOP));
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

    if (!cmdline.hasOption(INPUT) ||  !cmdline.hasOption(TOP) ||  !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }
    
    
    String inputPath = cmdline.getOptionValue(INPUT);
    int n = Integer.parseInt(cmdline.getOptionValue(TOP));
    String sourceValues = cmdline.getOptionValue(SOURCES);
    String sources[] = sourceValues.split(",");
    
    LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    LOG.info(" - input: " + inputPath);
    LOG.info(" - top: " + n);
    StringBuilder displaySources = new StringBuilder();
    for (int i = 0; i < sources.length; i++) {
      displaySources.append(sources[i] + " ");
    }
    LOG.info(" - sources are : " + displaySources);
    int numberOfSources = sources.length;
    LOG.info(" - number of sources :" + numberOfSources);
    
    // Initializing data structures to be used
    ArrayList<TopScoredObjects<String>> listOfQueues =
        new ArrayList<TopScoredObjects<String>>();
    // We need one queue for each source
    for (int i = 0; i < numberOfSources; i++) {
      listOfQueues.add(new TopScoredObjects<String>(n));
    }

    // Working over here
    SequenceFile.Reader seqFileReader;
    Configuration conf = getConf();
    Path path = new Path(inputPath);
    FileSystem fs = FileSystem.get(conf);
    for (FileStatus st : fs.listStatus(path)) {
      String fileName = st.getPath().getName();
      if (fileName.contains("_SUCCESS")) {
        continue;
      }
      // Read the sequence files
      seqFileReader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(st.getPath()));
      
      Text key = (Text) seqFileReader.getKeyClass().newInstance();
      PageRankNodeEnhanced node =
          (PageRankNodeEnhanced) seqFileReader.getValueClass().newInstance();

      while (seqFileReader.next(key, node)) {
        for (int i = 0; i < numberOfSources; i++) {
          TopScoredObjects<String> queue = listOfQueues.get(i);
          queue.add(key.toString(), node.getPageRankList().get(i));
          listOfQueues.set(i, queue);
        }
      }
      seqFileReader.close();
    }
    
    // Now printing the top ten pageranks and their nodes 
    for (int k=0;k<numberOfSources;k++) {
      System.out.println("Source: " + sources[k]);
      TopScoredObjects<String> queue = listOfQueues.get(k);
      for (PairOfObjectFloat<String> p : queue.extractAll()) {
        
        System.out.println(String.format("%.5f %s", StrictMath.exp(p.getRightElement()), p.getLeftElement()));
      }
      System.out.println();
    }
    return 0;
  }
  
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
  }

  

}
