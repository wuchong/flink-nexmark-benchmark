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
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.flink.benchmark.nexmark.NexmarkUtils;
import org.apache.flink.benchmark.nexmark.model.avro.AvroBid;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Objects;


/**
 * A bid for an item on auction.
 */
@DefaultSchema(JavaFieldSchema.class)
public class Bid implements KnownSize, Serializable {


    /**
     * Id of auction this bid is for.
     */
    @JsonProperty
    public long auction; // foreign key: Auction.id

    /**
     * Id of person bidding in auction.
     */
    @JsonProperty
    public long bidder; // foreign key: Person.id

    /**
     * Price of bid, in cents.
     */
    @JsonProperty
    public long price;

    /**
     * Instant at which bid was made (ms since epoch). NOTE: This may be earlier than the system's
     * event time.
     */
    @JsonProperty
    public Timestamp ts;

    /**
     * Additional arbitrary payload for performance testing.
     */
    @JsonProperty
    public String extra;

    // For Avro only.
    @SuppressWarnings("unused")
    public Bid() {
        auction = 0;
        bidder = 0;
        price = 0;
        ts = new Timestamp(0);
        extra = null;
    }

    public Bid(long auction, long bidder, long price, Timestamp timestamp, String extra) {
        this.auction = auction;
        this.bidder = bidder;
        this.price = price;
        this.ts = timestamp;
        this.extra = extra;
    }

    /**
     * Return a copy of bid which capture the given annotation. (Used for debugging).
     */
    public Bid withAnnotation(String annotation) {
        return new Bid(auction, bidder, price, ts, annotation + ": " + extra);
    }

    /**
     * Does bid have {@code annotation}? (Used for debugging.)
     */
    public boolean hasAnnotation(String annotation) {
        return extra.startsWith(annotation + ": ");
    }

    /**
     * Remove {@code annotation} from bid. (Used for debugging.)
     */
    public Bid withoutAnnotation(String annotation) {
        if (hasAnnotation(annotation)) {
            return new Bid(auction, bidder, price, ts, extra.substring(annotation.length() + 2));
        } else {
            return this;
        }
    }

    @Override
    public boolean equals(Object otherObject) {
        if (this == otherObject) {
            return true;
        }
        if (otherObject == null || getClass() != otherObject.getClass()) {
            return false;
        }

        Bid other = (Bid) otherObject;
        return Objects.equals(auction, other.auction)
                && Objects.equals(bidder, other.bidder)
                && Objects.equals(price, other.price)
                && Objects.equals(ts, other.ts)
                && Objects.equals(extra, other.extra);
    }

    @Override
    public int hashCode() {
        return Objects.hash(auction, bidder, price, ts, extra);
    }

    @Override
    public long sizeInBytes() {
        return 8L + 8L + 8L + 8L + extra.length() + 1L;
    }

    @Override
    public String toString() {
        try {
            return NexmarkUtils.MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public long getAuction() {
        return auction;
    }

    public void setAuction(long auction) {
        this.auction = auction;
    }

    public long getBidder() {
        return bidder;
    }

    public void setBidder(long bidder) {
        this.bidder = bidder;
    }

    public long getPrice() {
        return price;
    }

    public void setPrice(long price) {
        this.price = price;
    }

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public String getExtra() {
        return extra;
    }

    public void setExtra(String extra) {
        this.extra = extra;
    }


    /*
    auction = 0;
        bidder = 0;
        price = 0;
        ts = new Timestamp(0);
        extra = null;
     */
    public AvroBid toAvro(){
        AvroBid avroBid = AvroBid.newBuilder()
                .setAuction(this.auction)
                .setBidder(this.bidder)
                .setPrice(this.price)
                .setTs(this.ts.getTime())
                .setExtra(this.extra).build();
        return avroBid;
    }

    public Bid(AvroBid avroBid){
        this.auction = avroBid.getAuction();
        this.bidder = avroBid.getBidder();
        this.price = avroBid.getPrice();
        this.ts = new Timestamp(avroBid.getTs());
        this.extra = avroBid.getExtra().toString();
    }
}
