package net.hubs1.mahout.cluster;


/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.IOException;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericsUtil;
import org.apache.mahout.common.CommandLineUtil;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.iterator.FileLineIterable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.hubs1.mahout.cluster.CRLFInputFormat;
import net.hubs1.mahout.cluster.CRLFMapper;

/**
 * Create and run the Wikipedia Dataset Creator.
 */
public final class CRLFSeparatedToSequenceFile {

  private static final Logger log = LoggerFactory.getLogger(CRLFSeparatedToSequenceFile.class);
  
  private CRLFSeparatedToSequenceFile() { }
  
  /**
   * Takes in two arguments:
   * <ol>
   * <li>The input {@link org.apache.hadoop.fs.Path} where the input documents live</li>
   * <li>The output {@link org.apache.hadoop.fs.Path} where to write the classifier as a
   * {@link org.apache.hadoop.io.SequenceFile}</li>
   * </ol>
   */
  public static void main(String[] args) throws IOException {

    GroupBuilder gbuilder = new GroupBuilder();
    
    Option dirInputPathOpt = DefaultOptionCreator.inputOption().create();
    
    Option dirOutputPathOpt = DefaultOptionCreator.outputOption().create();
    
    Option helpOpt = DefaultOptionCreator.helpOption();
    
    Group group = gbuilder.withName("Options").withOption(dirInputPathOpt)
        .withOption(dirOutputPathOpt).withOption(helpOpt)
        .create();
    
    Parser parser = new Parser();
    parser.setGroup(group);
    parser.setHelpOption(helpOpt);
    try {
      CommandLine cmdLine = parser.parse(args);
      if (cmdLine.hasOption(helpOpt)) {
        CommandLineUtil.printHelp(group);
        return;
      }
      
      String inputPath = (String) cmdLine.getValue(dirInputPathOpt);
      String outputPath = (String) cmdLine.getValue(dirOutputPathOpt);
      
      runJob(inputPath, outputPath);
    } catch (OptionException e) {
      log.error("Exception", e);
      CommandLineUtil.printHelp(group);
    } catch (InterruptedException e) {
      log.error("Exception", e);
      CommandLineUtil.printHelp(group);
    } catch (ClassNotFoundException e) {
      log.error("Exception", e);
      CommandLineUtil.printHelp(group);
    }
  }
  
  /**
   * Run the job
   * 
   * @param input
   *          the input pathname String
   * @param output
   *          the output pathname String
   */
  public static void runJob(String input,
                            String output) throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();

    Job job = new Job(conf);
    if (log.isInfoEnabled()) {
      log.info("Input: {} Out: {} ", new Object[] {input, output});
    }
    
    job.setJobName("CRLF2Sequence");
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.setInputPaths(job, new Path(input));
    Path outPath = new Path(output);
    FileOutputFormat.setOutputPath(job, outPath);
    job.setMapperClass(CRLFMapper.class);
    job.setInputFormatClass(CRLFInputFormat.class);
    job.setReducerClass(Reducer.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setJarByClass(CRLFSeparatedToSequenceFile.class);
    
    /*
     * conf.set("mapred.compress.map.output", "true"); conf.set("mapred.map.output.compression.type",
     * "BLOCK"); conf.set("mapred.output.compress", "true"); conf.set("mapred.output.compression.type",
     * "BLOCK"); conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
     */
    HadoopUtil.delete(conf, outPath);
    
    
    boolean succeeded = job.waitForCompletion(true);
    if (!succeeded) {
      throw new IllegalStateException("Job failed!");
    }
  }
}
