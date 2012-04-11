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

import java.math.BigDecimal;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.cloudera.science.matching.graph.VertexState;
import com.google.common.collect.Maps;

/**
 * The serialized version of the data that we maintain about each vertex in our bipartite graph. The
 * Crunch and Giraph jobs communicate by reading/writing JSON-ified versions of these objects.
 */
public class VertexData {
  private String vertexId;
  private boolean bidder;
  private Map<String, Integer> edges;
  private Map<String, String> priceIndex;
  
  private String matchId;
  private String price;
  
  public VertexData() {
  }
  
  /**
   * The VertexData constructor used by Crunch's input preparation pipeline.
   * 
   * @param vertexId a unique identifier for this vertex
   * @param bidder whether this vertex represents a bidder or an object
   * @param edges The edges this vertex connects to, and for bidders, the weights.
   */
  public VertexData(String vertexId, boolean bidder, Map<String, Integer> edges) {
    this.vertexId = vertexId;
    this.bidder = bidder;
    this.edges = edges;
    this.matchId = "";
    this.price = "0";
    this.priceIndex = Maps.newHashMap();
  }

  public VertexData(Text vertexId, VertexState vertexState, Map<Text, IntWritable> edges) {
    this.vertexId = vertexId.toString();
    this.bidder = vertexState.isBidder();
    this.edges = Maps.newHashMap();
    for (Map.Entry<Text, IntWritable> e : edges.entrySet()) {
      this.edges.put(e.getKey().toString(), e.getValue().get());
    }
    this.priceIndex = Maps.newHashMap();
    for (Map.Entry<Text, BigDecimal> e : vertexState.getPriceIndex().entrySet()) {
      priceIndex.put(e.getKey().toString(), e.getValue().toString());
    }
    this.matchId = vertexState.getMatchId().toString();
    this.price = vertexState.getPrice().toString();
  }

  public String getVertexId() { return vertexId; }
  public void setVertexId(String vertexId) { this.vertexId = vertexId; }
  
  public boolean isBidder() { return bidder; }
  public void setBidder(boolean bidder) { this.bidder = bidder; }
  
  public Map<String, Integer> getEdges() { return edges; }
  public void setEdges(Map<String, Integer> edges) { this.edges = edges; }
  
  public Map<String, String> getPriceIndex() { return priceIndex; }
  public void setPriceIndex(Map<String, String> priceIndex) { this.priceIndex = priceIndex; }
  
  public String getMatchId() { return matchId; }
  public void setMatchId(String matchId) { this.matchId = matchId; }
  
  public String getPrice() { return price; }
  public void setPrice(String price) { this.price = price; }
  
  /**
   * Returns the Giraph-compatible version of the vertex identifier.
   * 
   * @return the Giraph-compatible version of the vertex identifier
   */
  public Text extractVertexId() {
    return new Text(vertexId);
  }
  
  /**
   * Constructs a {@link VertexState} from the information contained in this object.
   * 
   * @return a {@link VertexState} from the information contained in this object
   */
  public VertexState extractVertexState() {
    return new VertexState(bidder, new Text(matchId), new BigDecimal(price), extractPriceIndex());
  }
  
  private Map<Text, BigDecimal> extractPriceIndex() {
    Map<Text, BigDecimal> out = Maps.newHashMap();
    for (Map.Entry<String, String> e : priceIndex.entrySet()) {
      out.put(new Text(e.getKey()), new BigDecimal(e.getValue()));
    }
    return out;
  }
  
  /**
   * Returns the Giraph-compatible form of the edge data for this vertex.
   * 
   * @return the Giraph-compatible form of the edge data for this vertex
   */
  public Map<Text, IntWritable> extractEdges() {
    Map<Text, IntWritable> out = Maps.newHashMap();
    for (Map.Entry<String, Integer> e : edges.entrySet()) {
      out.put(new Text(e.getKey()), new IntWritable(e.getValue()));
    }
    return out;
  }
}
