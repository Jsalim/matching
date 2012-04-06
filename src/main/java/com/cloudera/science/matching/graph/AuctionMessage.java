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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Represents the messages that the nodes pass back and forth over the course of the
 * auction. The interpretation of the message depends on the recipient- bidders receive
 * prices from objects, and objects receive bids from bidders.
 */
public class AuctionMessage implements WritableComparable<AuctionMessage> {

  private Text vertexId;
  private double value;
  
  public AuctionMessage() { }
  
  public AuctionMessage(Text vertexId, double value) {
    this.vertexId = vertexId;
    this.value = value;
  }
  
  public Text getVertexId() {
    return vertexId;
  }
  
  public double getValue() {
    return value;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    if (vertexId == null) {
      vertexId = new Text();
    }
    vertexId.readFields(in);
    value = in.readDouble();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    vertexId.write(out);
    out.writeDouble(value);
  }

  @Override
  public int compareTo(AuctionMessage other) {
    return (int) (other.value - value);
  }
}
