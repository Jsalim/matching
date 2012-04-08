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
package com.cloudera.science.matching;

import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Maps;

public class VertexData {
  private String vertexId;
  private boolean bidder;
  private Map<String, Integer> edges;
  
  private String matchId;
  private double price;
  
  public VertexData() {
  }
  
  public VertexData(String vertexId, boolean bidder, Map<String, Integer> edges) {
    this.vertexId = vertexId;
    this.bidder = bidder;
    this.edges = edges;
    this.matchId = "";
    this.price = 0.0;
  }

  public VertexData(Text vertexId, VertexState vertexState, Map<Text, IntWritable> edges) {
    this.vertexId = vertexId.toString();
    this.bidder = vertexState.isBidder();
    this.edges = Maps.newHashMap();
    for (Map.Entry<Text, IntWritable> e : edges.entrySet()) {
      this.edges.put(e.getKey().toString(), e.getValue().get());
    }
    this.matchId = vertexState.getMatchId().toString();
    this.price = vertexState.getPrice();
  }

  public String getVertexId() { return vertexId; }
  public void setVertexId(String vertexId) { this.vertexId = vertexId; }
  
  public boolean isBidder() { return bidder; }
  public void setBidder(boolean bidder) { this.bidder = bidder; }
  
  public Map<String, Integer> getEdges() { return edges; }
  public void setEdges(Map<String, Integer> edges) { this.edges = edges; }
  
  public String getMatchId() { return matchId; }
  public void setMatchId(String matchId) { this.matchId = matchId; }
  
  public double getPrice() { return price; }
  public void setPrice(double price) { this.price = price; }
  
  public Text extractVertexId() {
    return new Text(vertexId);
  }
  
  public VertexState extractVertexState() {
    return new VertexState(bidder, new Text(matchId), price);
  }
  
  public Map<Text, IntWritable> extractEdges() {
    Map<Text, IntWritable> out = Maps.newHashMap();
    for (Map.Entry<String, Integer> e : edges.entrySet()) {
      out.put(new Text(e.getKey()), new IntWritable(e.getValue()));
    }
    return out;
  }
}
