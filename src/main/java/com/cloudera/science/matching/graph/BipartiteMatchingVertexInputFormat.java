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

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.codehaus.jackson.map.ObjectMapper;

import com.cloudera.science.matching.VertexData;


/**
 * Input format for the BipartiteMatchingVertex.
 */
public class BipartiteMatchingVertexInputFormat extends
    TextVertexInputFormat<Text, VertexState, IntWritable, AuctionMessage> {
  @Override
  public VertexReader<Text, VertexState, IntWritable, AuctionMessage> createVertexReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new BipartiteMatchingVertexReader(textInputFormat.createRecordReader(split, context));
  }
  
  public static class BipartiteMatchingVertexReader extends TextVertexReader<Text, VertexState, IntWritable, AuctionMessage> {
    private ObjectMapper mapper;
    
    public BipartiteMatchingVertexReader(RecordReader<LongWritable, Text> rr) {
      super(rr);
      this.mapper = new ObjectMapper();
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }

    @Override
    public BasicVertex<Text, VertexState, IntWritable, AuctionMessage> getCurrentVertex()
        throws IOException, InterruptedException {
      VertexData vertexData = mapper.readValue(getRecordReader().getCurrentValue().toString(), VertexData.class);
      BipartiteMatchingVertex v = new BipartiteMatchingVertex();
      v.initialize(vertexData.extractVertexId(), vertexData.extractVertexState(), vertexData.extractEdges(), null);
      return v;
    }
  }
}
