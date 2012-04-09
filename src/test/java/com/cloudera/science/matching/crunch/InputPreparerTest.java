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

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.impl.mem.MemPipeline;
import com.cloudera.science.matching.VertexData;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class InputPreparerTest {

  private InputPreparer prep = new InputPreparer();
  
  @Test
  public void testInputPrep() {
    PCollection<String> input = MemPipeline.collectionOf("1,3,1", "1,4,2", "2,3,1", "2,4,3");
    PCollection<VertexData> out = prep.exec(input, ",");
    List<VertexData> data = Lists.newArrayList(out.materialize());
    assertEquals(4, data.size());
    for (int i = 0; i < 4; i++) {
      VertexData d = data.get(i);
      if ("1".equals(d.getVertexId())) {
        assertEquals(ImmutableMap.of("3", 1, "4", 2), d.getEdges());
      } else if ("2".equals(d.getVertexId())) {
        assertEquals(ImmutableMap.of("3", 1, "4", 3), d.getEdges());
      } else {
        assertEquals(ImmutableMap.of("1", -1, "2", -1), d.getEdges());
      }
    }
  }
}
