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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.io.From;
import com.cloudera.crunch.io.To;
import com.cloudera.crunch.type.PType;
import com.cloudera.crunch.type.writable.WritableTypeFamily;
import com.cloudera.crunch.type.writable.Writables;
import com.cloudera.crunch.util.PTypes;
import com.cloudera.science.matching.VertexData;

/**
 * Processes the output of the Giraph job and emits the matched pairs.
 */
public class OutputProcessor extends Configured implements Tool {

  private static final PType<VertexData> vType = PTypes.jsonString(VertexData.class, WritableTypeFamily.getInstance());
  
  public static PCollection<String> exec(PCollection<VertexData> giraphOutput) {
    return giraphOutput.parallelDo(new DoFn<VertexData, String>() {
      @Override
      public void process(VertexData input, Emitter<String> emitter) {
        if (input.isBidder()) {
          String matchId = input.getMatchId();
          Integer score = input.getEdges().get(matchId);
          emitter.emit(String.format("%s,%s,%s", input.getVertexId(), matchId, score));
        }
      }
    }, Writables.strings());
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: <input> <output>");
    }
    Pipeline p = new MRPipeline(OutputProcessor.class, getConf());
    exec(p.read(From.textFile(args[0], vType))).write(To.textFile(args[1]));
    p.done();
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new OutputProcessor(), args);
  }
}
