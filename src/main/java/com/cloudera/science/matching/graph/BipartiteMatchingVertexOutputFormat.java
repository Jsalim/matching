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
import org.apache.giraph.graph.VertexWriter;
import org.apache.giraph.lib.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.codehaus.jackson.map.ObjectMapper;

import com.cloudera.science.matching.VertexData;


/**
 * OutputFormat for BipartiteMatchingVertex.
 */
public class BipartiteMatchingVertexOutputFormat extends
    TextVertexOutputFormat<Text, VertexState, IntWritable> {
  @Override
  public VertexWriter<Text, VertexState, IntWritable> createVertexWriter(
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new BipartiteMatchingVertexWriter(textOutputFormat.getRecordWriter(context));
  }
  
  public static class BipartiteMatchingVertexWriter extends TextVertexWriter<Text, VertexState, IntWritable> {
    private static final Text BLANK = new Text("");
    
    private ObjectMapper mapper;
    
    public BipartiteMatchingVertexWriter(RecordWriter<Text, Text> lineRecordWriter) {
      super(lineRecordWriter);
      this.mapper = new ObjectMapper();
    }
    
    @Override
    public void writeVertex(BasicVertex<Text, VertexState, IntWritable, ?> vertex)
        throws IOException, InterruptedException {
      BipartiteMatchingVertex bmv = (BipartiteMatchingVertex) vertex;
      VertexData vertexData = new VertexData(bmv.getVertexId(), bmv.getVertexValue(), bmv.getEdges());
      getRecordWriter().write(BLANK, new Text(mapper.writeValueAsString(vertexData)));
    }
  }
}
