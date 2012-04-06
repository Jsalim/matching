/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.science.matching.graph;

import java.io.IOException;
import java.util.List;

import org.apache.giraph.graph.VertexInputFormat;
import org.apache.giraph.graph.VertexReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import com.cloudera.science.matching.VertexDataWritable;

/**
 *
 */
public class BipartiteMatchingVertexInputFormat extends
    VertexInputFormat<Text, VertexState, IntWritable, AuctionMessage> {

  private final SequenceFileInputFormat<Text, VertexDataWritable> inputFormat;
  
  public BipartiteMatchingVertexInputFormat() {
    this.inputFormat = new SequenceFileInputFormat<Text, VertexDataWritable>();
  }
  
  @Override
  public List<InputSplit> getSplits(JobContext context, int numWorkers)
      throws IOException, InterruptedException {
    return inputFormat.getSplits(context);
  }

  @Override
  public VertexReader<Text, VertexState, IntWritable, AuctionMessage> createVertexReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new BipartiteMatchingVertexReader(inputFormat.createRecordReader(split, context));
  }

}
