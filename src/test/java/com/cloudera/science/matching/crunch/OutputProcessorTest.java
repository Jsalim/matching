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

public class OutputProcessorTest {
  @Test
  public void testOutputProcessor() {
    VertexData d1 = new VertexData("1", true, ImmutableMap.of("3", 2, "4", 1)); 
    VertexData d2 = new VertexData("2", true, ImmutableMap.of("3", 1, "4", 2));
    VertexData d3 = new VertexData("3", false, ImmutableMap.of("1", -1, "2", -1));
    VertexData d4 = new VertexData("4", false, ImmutableMap.of("1", -1, "2", -1));
    d1.setMatchId("3");
    d2.setMatchId("4");
    d3.setMatchId("1");
    d4.setMatchId("2");
    
    PCollection<VertexData> data = MemPipeline.collectionOf(d1, d2, d3, d4);
    List<String> out = Lists.newArrayList(OutputProcessor.exec(data).materialize());
    assertEquals(2, out.size());
    assertEquals("1,3", out.get(0));
    assertEquals("2,4", out.get(1));
  }
}
