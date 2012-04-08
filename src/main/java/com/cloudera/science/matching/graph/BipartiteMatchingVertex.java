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

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.giraph.graph.EdgeListVertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.cloudera.science.matching.VertexState;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 *
 */
public class BipartiteMatchingVertex extends EdgeListVertex<Text, VertexState, IntWritable, AuctionMessage> {
  
  public Map<Text, IntWritable> getEdges() {
    Map<Text, IntWritable> out = Maps.newHashMap();
    for (Text vertexId : this) {
      out.put(vertexId, getEdgeValue(vertexId));
    }
    return out;
  }
  
  @Override
  public void compute(Iterator<AuctionMessage> msgIterator) throws IOException {
    long superstep = getSuperstep();
    VertexState state = getVertexValue();
    if (state.isBidder()) {
      if (superstep % 2 == 0) {
        // Need to track who I own.
        VertexPriceData vpd = new VertexPriceData(msgIterator);
        if (vpd.newMatchedId != null) {
          state.setMatchId(vpd.newMatchedId);
        } else if (vpd.newLostId != null) {
          state.setMatchId(null);
        }
        List<AuctionMessage> values = Lists.newArrayList();
        for (Text vertexId : this) {
          double value = getEdgeValue(vertexId).get() - vpd.getPrice(vertexId);
          values.add(new AuctionMessage(vertexId, value));
        }
        Collections.sort(values);
        AuctionMessage target = values.get(0);
        Text currentMatchId = state.getMatchId();
        if (currentMatchId == null || !currentMatchId.equals(target.getVertexId())) {
          double bid = Double.POSITIVE_INFINITY;
          if (values.size() > 1) {
            double epsilon = 2.0 / (this.getNumVertices() + 1.0);
            AuctionMessage runnerUp = values.get(1);
            double inc = target.getValue() - runnerUp.getValue() + epsilon;
            bid = vpd.getPrice(target.getVertexId()) + inc;
          }
          sendMsg(target.getVertexId(), newMsg(bid));
          if (currentMatchId != null && !currentMatchId.toString().isEmpty()) {
            sendMsg(currentMatchId, newMsg(Double.NEGATIVE_INFINITY));
          }
        } else {
          // Otherwise, I'm happy.
          this.voteToHalt();
        }
      }
    } else {
      if (superstep % 2 == 1) {
        double price = state.getPrice();
        List<AuctionMessage> bids = sortBids(msgIterator);
        if (!bids.isEmpty()) {
          Text currentMatchId = state.getMatchId();
          AuctionMessage winningBid = bids.get(0);
          Text newMatchId = winningBid.getVertexId();
          if (currentMatchId == null || (!currentMatchId.equals(newMatchId) && winningBid.getValue() > price)) {
            state.setMatchId(newMatchId);
            state.setPrice(winningBid.getValue());
            // Need to send the owners a heads up.
            if (currentMatchId != null && !currentMatchId.toString().isEmpty()) {
              sendMsg(currentMatchId, newMsg(Double.NEGATIVE_INFINITY));
            }
            sendMsg(newMatchId, newMsg(Double.POSITIVE_INFINITY));
          }
          // Announce my price to all the bidders.
          sendMsgToAllEdges(newMsg(state.getPrice()));
        }
      } else {
        // Objects always vote to halt on mod zero iterations.
        this.voteToHalt();
      }
    }
  }

  static class VertexPriceData {
    public Map<Text, Double> prices;
    public Text newMatchedId;
    public Text newLostId;
    
    public VertexPriceData(Iterator<AuctionMessage> iter) {
      this.prices = Maps.newHashMap();
      while (iter.hasNext()) {
        AuctionMessage msg = iter.next();
        if (msg.getValue() == Double.POSITIVE_INFINITY) {
          newMatchedId = msg.getVertexId();
        } else if (msg.getValue() == Double.NEGATIVE_INFINITY) {
          newLostId = msg.getVertexId(); 
        } else {
          prices.put(msg.getVertexId(), msg.getValue());
        }
      }
    }
    
    public Double getPrice(Text vertexId) {
      return prices.containsKey(vertexId) ? prices.get(vertexId) : 0.0;
    }
  }
  
  private AuctionMessage newMsg(double value) {
    return new AuctionMessage(getVertexId(), value);
  }
  
  private List<AuctionMessage> sortBids(Iterator<AuctionMessage> msgIterator) {
    List<AuctionMessage> bids = Lists.newArrayList(msgIterator);
    Collections.sort(bids);
    return bids;
  }
}
