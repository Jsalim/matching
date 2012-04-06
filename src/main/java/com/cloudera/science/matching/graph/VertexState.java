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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.Lists;

/**
 * Maintains the internal state of a vertex in the bipartite graph, with 
 */
public class VertexState implements Writable {

  private boolean bidder;
  private Text ownerId = new Text();
  private List<Text> ownedIds = Lists.newArrayListWithCapacity(0);
  private double price = 0.0;
  
  public VertexState() { }
  
  public VertexState(boolean bidder) {
    this.bidder = bidder;
  }
  
  public boolean isBidder() {
    return bidder;
  }
  
  public Text getOwnerId() {
    return ownerId;
  }
  
  public void setOwnerId(Text ownerId) {
    this.ownerId = ownerId;
  }
  
  public List<Text> getOwnedIds() {
    return ownedIds;
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
    if (bidder) {
      ownedIds.clear();
      int sz = WritableUtils.readVInt(in);
      for (int i = 0; i < sz; i++) {
        Text ownedId = new Text();
        ownedId.readFields(in);
        ownedIds.add(ownedId);
      }
    } else {
      ownerId.readFields(in);
      price = in.readDouble();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(bidder);
    if (bidder) {
      WritableUtils.writeVInt(out, ownedIds.size());
      for (Text ownedId : ownedIds) {
        ownedId.write(out);
      }
    } else {
      ownerId.write(out);
      out.writeDouble(price);
    }
  }
}
