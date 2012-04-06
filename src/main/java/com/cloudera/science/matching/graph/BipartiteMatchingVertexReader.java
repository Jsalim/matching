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
import java.util.Map;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.VertexReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.cloudera.science.matching.VertexDataWritable;
import com.google.common.collect.Maps;

/**
 *
 */
public class BipartiteMatchingVertexReader implements VertexReader<Text, VertexState, IntWritable, AuctionMessage> {

  private static final IntWritable NEG_ONE = new IntWritable(-1);
  
  private RecordReader<Text, VertexDataWritable> rr;
  
  public BipartiteMatchingVertexReader(RecordReader<Text, VertexDataWritable> recordReader) {
    this.rr = recordReader;
  }
  
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
    rr.initialize(inputSplit, context);
  }

  @Override
  public boolean nextVertex() throws IOException, InterruptedException {
    return rr.nextKeyValue();
  }

  @Override
  public BasicVertex<Text, VertexState, IntWritable, AuctionMessage> getCurrentVertex()
      throws IOException, InterruptedException {
    Text vertexId = rr.getCurrentKey();
    VertexDataWritable vdw = rr.getCurrentValue();
    
    Map<Text, IntWritable> edges = Maps.newHashMap();
    Text[] others = vdw.getTargets();
    int[] edgeWeights = vdw.getValues();
    for (int i = 0; i < others.length; i++) {
      edges.put(others[i], edgeWeights == null ? NEG_ONE : new IntWritable(edgeWeights[i]));
    }
    BipartiteMatchingVertex v = new BipartiteMatchingVertex();
    v.initialize(vertexId, new VertexState(vdw.isBidder()), edges, null);
    return v;
  }

  @Override
  public void close() throws IOException {
    rr.close();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return rr.getProgress();
  }

}
