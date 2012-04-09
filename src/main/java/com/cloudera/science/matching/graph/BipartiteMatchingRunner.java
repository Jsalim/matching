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

import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 */
public class BipartiteMatchingRunner extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: <input> <output> <numworkers>");
      return 1;
    }
    
    GiraphJob job = new GiraphJob(getConf(), getClass().getName());
    
    job.setVertexClass(BipartiteMatchingVertex.class);
    job.setVertexInputFormatClass(BipartiteMatchingVertexInputFormat.class);
    job.setVertexOutputFormatClass(BipartiteMatchingVertexOutputFormat.class);
    FileInputFormat.addInputPath(job.getInternalJob(), new Path(args[0]));
    FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(args[1]));
    
    int numWorkers = Integer.parseInt(args[2]);
    job.setWorkerConfiguration(numWorkers, numWorkers, 100.0f);
    
    return job.run(true) ? 0 : -1;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BipartiteMatchingRunner(), args);
  }
}
