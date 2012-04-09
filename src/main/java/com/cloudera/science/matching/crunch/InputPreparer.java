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

import static com.cloudera.crunch.type.avro.Avros.*;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.io.From;
import com.cloudera.crunch.io.To;
import com.cloudera.crunch.type.writable.WritableTypeFamily;
import com.cloudera.crunch.util.PTypes;
import com.cloudera.science.matching.VertexData;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
      Integer score = Integer.valueOf(pieces.get(2));
      emitter.emit(Pair.of(id1, Pair.of(id2, score)));
      emitter.emit(Pair.of(id2, Pair.of(id1, -1)));
    }
  }
  
  public static class WriteVertexFn extends MapFn<Pair<String, Iterable<Pair<String, Integer>>>, VertexData> {
    @Override
    public VertexData map(Pair<String, Iterable<Pair<String, Integer>>> v) {
      List<Pair<String, Integer>> pairs = Lists.newArrayList(v.second());
      Map<String, Integer> targets = Maps.newHashMap();
      boolean bidder = true;
      for (int i = 0; i < pairs.size(); i++) {
        String id = pairs.get(i).first();
        Integer score = pairs.get(i).second();
        if (i == 0) {
          if (score < 0) {
            bidder = false;
          }
        } else if (bidder && score < 0) {
          throw new IllegalStateException(
              String.format("Invalid input: vertex id %s occurs in both sides of the graph", id));
        } else if (!bidder && score >= 0) {
          throw new IllegalStateException(
              String.format("Invalid input: vertex id %s occurs in both sides of the graph", id));
        }
        targets.put(id, score);
      }
      return new VertexData(v.first(), bidder, targets);
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

  public PCollection<VertexData> exec(PCollection<String> input, String sep) {
    return input
        .parallelDo(new TwoVerticesFn(sep), tableOf(strings(), pairs(strings(), ints())))
        .groupByKey()
        .parallelDo(new WriteVertexFn(), PTypes.jsonString(VertexData.class, WritableTypeFamily.getInstance()));
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: <input> <output> <sepchar>");
      return 1;
    }
    
    Pipeline p = new MRPipeline(InputPreparer.class, conf);
    exec(p.read(From.textFile(args[0])), args[2]).write(To.textFile(args[1]));
    p.done();
    
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new InputPreparer(), args);
  }
}
