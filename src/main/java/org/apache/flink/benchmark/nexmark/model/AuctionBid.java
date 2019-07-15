/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.benchmark.nexmark.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.Serializable;

import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Objects;
import org.apache.flink.benchmark.nexmark.NexmarkUtils;


/** Result of {@link WinningBids} transform. */
public class AuctionBid implements KnownSize, Serializable {


  @JsonProperty
  public Auction auction;

  @JsonProperty
  public Bid bid;

  @SuppressWarnings("unused")
  public AuctionBid() {
    auction = null;
    bid = null;
  }

  public AuctionBid(Auction auction, Bid bid) {
    this.auction = auction;
    this.bid = bid;
  }

  @Override
  public long sizeInBytes() {
    return auction.sizeInBytes() + bid.sizeInBytes();
  }

  @Override
  public String toString() {
    try {
      return NexmarkUtils.MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuctionBid that = (AuctionBid) o;
    return Objects.equal(auction, that.auction) && Objects.equal(bid, that.bid);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(auction, bid);
  }
}
