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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 *
 */
public class BipartiteMatchingVertex extends EdgeListVertex<Text, VertexState, IntWritable, AuctionMessage> {
  @Override
  public void compute(Iterator<AuctionMessage> msgIterator) throws IOException {
    long superstep = getSuperstep();
    VertexState state = getVertexValue();
    if (state.isBidder()) {
      if (superstep % 2 == 0) {
        // Need to track who I own.
        Map<Text, Double> prices = getVertexPrices(msgIterator);
        List<AuctionMessage> values = Lists.newArrayList();
        for (Text vertexId : this) {
          int edgeWeight = getEdgeValue(vertexId).get();
          double value = prices.containsKey(vertexId) ? edgeWeight - prices.get(vertexId) : edgeWeight;
          values.add(new AuctionMessage(vertexId, value));
        }
        Collections.sort(values);
        AuctionMessage target = values.get(0);
        double bid = Double.POSITIVE_INFINITY;
        if (values.size() > 1) {
          AuctionMessage runnerUp = values.get(1);
          double price = prices.containsKey(target.getVertexId()) ? prices.get(target.getVertexId()) : 0.0;
          bid = price + target.getValue() - runnerUp.getValue(); // + epsilon
        }
        sendMsg(target.getVertexId(), new AuctionMessage(getVertexId(), bid));
      } else {
        this.voteToHalt();
      }
    } else {
      if (superstep % 2 == 1) {
        double price = state.getPrice();
        List<AuctionMessage> bids = sortBids(msgIterator);
        if (!bids.isEmpty()) {
          Text ownerId = state.getOwnerId();
          AuctionMessage winningBid = bids.get(0);
          Text newOwnerId = winningBid.getVertexId();
          if (ownerId == null || (!ownerId.equals(newOwnerId) && winningBid.getValue() > price)) {
            state.setOwnerId(newOwnerId);
            state.setPrice(winningBid.getValue());
            // Announce my new price to all the bidders.
            sendMsgToAllEdges(new AuctionMessage(getVertexId(), state.getPrice()));
            // Need to send the owner the info that he owns me.
          }
        }
      } else {
        this.voteToHalt();
      }
    }
  }

  private Map<Text, Double> getVertexPrices(Iterator<AuctionMessage> msgIterator) {
    Map<Text, Double> prices = Maps.newHashMap();
    while (msgIterator.hasNext()) {
      AuctionMessage msg = msgIterator.next();
      prices.put(msg.getVertexId(), msg.getValue());
    }
    return prices;
  }
  
  private List<AuctionMessage> sortBids(Iterator<AuctionMessage> msgIterator) {
    List<AuctionMessage> bids = Lists.newArrayList(msgIterator);
    Collections.sort(bids);
    return bids;
  }
}
