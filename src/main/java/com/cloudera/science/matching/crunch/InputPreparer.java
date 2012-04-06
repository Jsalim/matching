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
package com.cloudera.science.matching.crunch;

import static com.cloudera.crunch.type.writable.Writables.*;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.fn.MapValuesFn;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.io.From;
import com.cloudera.crunch.io.To;
import com.cloudera.science.matching.VertexDataWritable;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/**
 *
 */
public class InputPreparer implements Tool {

  public static class TwoVerticesFn extends DoFn<String, Pair<String, Pair<String, Integer>>> {
    
    private final String sep;
    
    public TwoVerticesFn(String sep) {
      this.sep = sep;
    }
    
    @Override
    public void process(String input, Emitter<Pair<String, Pair<String, Integer>>> emitter) {
      List<String> pieces = Lists.newArrayList(Splitter.on(sep).split(input));
      String id1 = pieces.get(0);
      String id2 = pieces.get(1);
      Integer score = Integer.valueOf(pieces.get(1));
      emitter.emit(Pair.of(id1, Pair.of(id2, score)));
      emitter.emit(Pair.of(id2, Pair.of(id1, -1)));
    }
  }
  
  public static class WriteVertexFn extends MapValuesFn<String, Iterable<Pair<String, Integer>>, VertexDataWritable> {
    @Override
    public VertexDataWritable map(Iterable<Pair<String, Integer>> v) {
      List<Pair<String, Integer>> pairs = Lists.newArrayList(v);
      int[] values = new int[pairs.size()];
      Text[] targets = new Text[pairs.size()];
      for (int i = 0; i < pairs.size(); i++) {
        Pair<String, Integer> p = pairs.get(i);
        targets[i] = new Text(p.first());
        values[i] = p.second();
      }
      boolean bidder = true;
      for (int i = 0; i < values.length; i++) {
        if (values[i] < 0) {
          if (i == 0) {
            bidder = false;
          } else if (bidder) {
            throw new IllegalStateException("Invalid input: vertex id occurs in both sides of the graph");
          }
        } else if (!bidder) {
          throw new IllegalStateException("Invalid input: vertex id occurs in both sides of the graph");
        }
      }
      return new VertexDataWritable(bidder, targets, values);
    }
  }
  
  private Configuration conf;
  
  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: <input> <output> <sepchar>");
      return 1;
    }
    Pipeline p = new MRPipeline(InputPreparer.class, conf);
    p.read(From.textFile(args[0]))
      .parallelDo(new TwoVerticesFn(args[2]), tableOf(strings(), pairs(strings(), ints())))
      .groupByKey()
      .parallelDo(new WriteVertexFn(), tableOf(strings(), writables(VertexDataWritable.class)))
      .write(To.sequenceFile(args[1]));
    p.done();
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new InputPreparer(), args);
  }
}
