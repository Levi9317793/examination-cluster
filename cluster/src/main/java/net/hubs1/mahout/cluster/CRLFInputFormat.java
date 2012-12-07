package net.hubs1.mahout.cluster;


/**
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

import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reads records that are delimited by a specific begin/end tag.
 */
public class CRLFInputFormat extends  TextInputFormat {

  private static final Logger log = LoggerFactory.getLogger(CRLFInputFormat.class);

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
    try {
      return new CRLFRecordReader((FileSplit) split, context.getConfiguration());
    } catch (IOException ioe) {
      log.warn("Error while creating CRLFRecordReader", ioe);
      return null;
    }
  }

  /**
   * CRLFRecordReader class to read through a given xml document to output xml blocks as records as specified
   * by the start tag and end tag
   * 
   */
  public static class CRLFRecordReader extends RecordReader<LongWritable, Text> {
	private static final Logger log = LoggerFactory.getLogger(CRLFRecordReader.class);
    private final long start;
    private final long end;
    private final FSDataInputStream fsin;
    private final LineReader lr;
    private LongWritable currentKey;
    private Text currentValue;

    public CRLFRecordReader(FileSplit split, Configuration conf) throws IOException {
      // open the file and seek to the start of the split
      log.info("begin create record reader");
      start = split.getStart();
      end = start + split.getLength();
      Path file = split.getPath();
      FileSystem fs = file.getFileSystem(conf);
      log.info("open file"+split.getPath());
      fsin = fs.open(split.getPath());
      fsin.seek(start);
      lr = new LineReader(fsin, conf);
      readUntilBlankLine();
      log.info("end create record reader,FileSplit  start=["+start+"] end=["+end+"]");
    }

    private boolean next(LongWritable key, Text value) throws IOException {
      if (fsin.getPos() < end) {
	    if (readRecord(value)) {
	      key.set(fsin.getPos());
	      log.info("read a record=["+value.toString()+"]");
	      return true;
	    }
	  }
      return false;
    }

    @Override
    public void close() throws IOException {
      Closeables.closeQuietly(fsin);
    }

    @Override
    public float getProgress() throws IOException {
      return (fsin.getPos() - start) / (float) (end - start);
    }

    
    private boolean isBlankLine(Text line){
    	if(line.toString().trim().length()==0) {    		
    		log.info(" is blank line="+line.toString());
    		return true;	
    	}
    	else
    		return false;
    }
    
    private boolean readUntilBlankLine() throws IOException {
  		Text line = new Text();
  		
  		//skip blank lines
		while (fsin.getPos() < end) {
			//line.clear();
			if(lr.readLine(line) <= 0) break;
			log.info("read a line="+line.toString()+" start="+fsin.getPos()+" end="+end);
			if(!isBlankLine(line)){
				continue;
			}
			else{
				return true;
			}
		}
	
		return false;
    }
    private boolean readRecord(Text value) throws IOException {
  		Text line = new Text();
  		//skip blank lines
		while (fsin.getPos() < end) {
			//line.clear();
			if(lr.readLine(line) <= 0) break;
			if(isBlankLine(line)){
				continue;
			}
			else{
				if(value != null){
					log.info("read line = ["+line.toString()+"]");
					value.append(line.getBytes(), 0, line.getLength());
					value.append("\n".getBytes(), 0, 1);
					log.info("value = ["+value.toString()+"]");
				}
				break;
			}
		}
		
		//read lines until blank line
		while (fsin.getPos() < end) {
			//line.clear();
			if(lr.readLine(line) <= 0) break;
			if(!isBlankLine(line)){
				log.info("read line = ["+line.toString()+"]");
				value.append(line.getBytes(), 0, line.getLength());
				value.append("\n".getBytes(), 0, 1);
				log.info("value = ["+value.toString()+"]");
			}
			else{
				return true;
			}			
		}
		return false;
    }
    
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
      return currentKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      return currentValue;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      currentKey = new LongWritable();
      currentValue = new Text();
      return next(currentKey, currentValue);
    }
  }
}
