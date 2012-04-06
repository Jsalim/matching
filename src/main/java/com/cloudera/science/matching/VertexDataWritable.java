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
import org.apache.hadoop.io.WritableUtils;

/**
 *
 */
public class VertexDataWritable implements Writable {

  private boolean bidder;
  private Text[] targets;
  private int[] values;
  
  public VertexDataWritable() {
  }
  
  public VertexDataWritable(boolean bidder, Text[] targets, int[] values) {
    this.bidder = bidder;
    this.targets = targets;
    this.values = values;
  }
  
  public boolean isBidder() {
    return bidder;
  }
  
  public Text[] getTargets() {
    return targets;
  }
  
  public int[] getValues() {
    return values;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    this.bidder = in.readBoolean();
    int sz = WritableUtils.readVInt(in);
    targets = new Text[sz];
    for (int i = 0; i < sz; i++) {
      targets[i] = new Text();
      targets[i].readFields(in);
    }
    if (bidder) {
      this.values = new int[sz];
      for (int i = 0; i < sz; i++) {
        values[i] = WritableUtils.readVInt(in);
      }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(bidder);
    WritableUtils.writeVInt(out, targets.length);
    for (int i = 0; i < targets.length; i++) {
      targets[i].write(out);
    }
    if (bidder) {
      for (int i = 0; i < values.length; i++) {
        WritableUtils.writeVInt(out, values[i]);
      }
    }
  }

}
