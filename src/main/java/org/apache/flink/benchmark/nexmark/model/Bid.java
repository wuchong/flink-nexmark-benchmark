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
import java.util.*;


import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.benchmark.nexmark.NexmarkUtils;
import org.apache.flink.table.api.Types;


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
    public long timestamp;

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
        timestamp = 0;
        extra = null;
    }

    public Bid(long auction, long bidder, long price, long timestamp, String extra) {
        this.auction = auction;
        this.bidder = bidder;
        this.price = price;
        this.timestamp = timestamp;
        this.extra = extra;
    }

    /**
     * Return a copy of bid which capture the given annotation. (Used for debugging).
     */
    public Bid withAnnotation(String annotation) {
        return new Bid(auction, bidder, price, timestamp, annotation + ": " + extra);
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
            return new Bid(auction, bidder, price, timestamp, extra.substring(annotation.length() + 2));
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
                && Objects.equals(timestamp, other.timestamp)
                && Objects.equals(extra, other.extra);
    }

    @Override
    public int hashCode() {
        return Objects.hash(auction, bidder, price, timestamp, extra);
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

    public static String[] getFieldNames() {
        return new String[]{"auction", "bidder", "price",
                "timestamp","extra"};
    }

    public static TypeInformation[] getFieldTypes() {

        return new TypeInformation[]{Types.LONG(), Types.LONG(), Types.LONG(),
                Types.LONG(), Types.STRING()};


    }


}
