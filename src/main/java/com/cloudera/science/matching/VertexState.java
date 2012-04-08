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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Maintains the internal state of a vertex in the bipartite graph, with 
 */
public class VertexState implements Writable {

  private boolean bidder;
  private Text matchId = new Text();
  private double price = 0.0;
  
  public VertexState() { }
  
  public VertexState(boolean bidder) {
    this.bidder = bidder;
  }
  
  public VertexState(boolean bidder, Text matchId, double price) {
    this.bidder = bidder;
    this.matchId = matchId;
    this.price = price;
  }
  
  public boolean isBidder() {
    return bidder;
  }
  
  public Text getMatchId() {
    return matchId;
  }
  
  public void setMatchId(Text ownerId) {
    this.matchId = ownerId;
  }
  
  public double getPrice() {
    return price;
  }
  
  public void setPrice(double price) {
    this.price = price;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    bidder = in.readBoolean();
    if (!bidder) {
      price = in.readDouble();
    }
    matchId.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(bidder);
    if (!bidder) {
      out.writeDouble(price);
    }
    matchId.write(out);
  }
}
