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


import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Objects;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.benchmark.nexmark.NexmarkUtils;
import org.apache.flink.table.api.Types;


/**
 * An auction submitted by a person.
 */
@DefaultSchema(JavaFieldSchema.class)
public class Auction implements KnownSize, Serializable {

    /**
     * Id of auction.
     */
    @JsonProperty
    public long id; // primary key

    /**
     * Extra auction properties.
     */
    @JsonProperty
    public String itemName;

    @JsonProperty
    public String description;

    /**
     * Initial bid price, in cents.
     */
    @JsonProperty
    public long initialBid;

    /**
     * Reserve price, in cents.
     */
    @JsonProperty
    public long reserve;

    @JsonProperty
    public long ts;

    /**
     * When does auction expire? (ms since epoch). Bids at or after this time are ignored.
     */
    @JsonProperty
    public long expires;

    /**
     * Id of person who instigated auction.
     */
    @JsonProperty
    public long seller; // foreign key: Person.id

    /**
     * Id of category auction is listed under.
     */
    @JsonProperty
    public long category; // foreign key: Category.id

    /**
     * Additional arbitrary payload for performance testing.
     */
    @JsonProperty
    public String extra;

    // For Avro only.
    @SuppressWarnings("unused")
    public Auction() {
        id = 0;
        itemName = null;
        description = null;
        initialBid = 0;
        reserve = 0;
        ts = 0;
        expires = 0;
        seller = 0;
        category = 0;
        extra = null;
    }

    public Auction(
            long id,
            String itemName,
            String description,
            long initialBid,
            long reserve,
            long tiemstamp,
            long expires,
            long seller,
            long category,
            String extra) {
        this.id = id;
        this.itemName = itemName;
        this.description = description;
        this.initialBid = initialBid;
        this.reserve = reserve;
        this.ts = tiemstamp;
        this.expires = expires;
        this.seller = seller;
        this.category = category;
        this.extra = extra;
    }


    public static String[] getFieldNames() {
        return new String[]{"id", "itemName", "description", "initialBid",
                "reserve", "ts", "expires", "seller", "category", "extra"};
    }

    public static TypeInformation[] getFieldTypes() {
        return new TypeInformation[]{Types.LONG(), Types.STRING(), Types.STRING(), Types.LONG(),
                Types.LONG(), Types.LONG(), Types.LONG(), Types.LONG(), Types.LONG(), Types.STRING()};
    }

    /**
     * Return a copy of auction which capture the given annotation. (Used for debugging).
     */
    public Auction withAnnotation(String annotation) {
        return new Auction(
                id,
                itemName,
                description,
                initialBid,
                reserve,
                ts,
                expires,
                seller,
                category,
                annotation + ": " + extra);
    }

    /**
     * Does auction have {@code annotation}? (Used for debugging.)
     */
    public boolean hasAnnotation(String annotation) {
        return extra.startsWith(annotation + ": ");
    }

    /**
     * Remove {@code annotation} from auction. (Used for debugging.)
     */
    public Auction withoutAnnotation(String annotation) {
        if (hasAnnotation(annotation)) {
            return new Auction(
                    id,
                    itemName,
                    description,
                    initialBid,
                    reserve,
                    ts,
                    expires,
                    seller,
                    category,
                    extra.substring(annotation.length() + 2));
        } else {
            return this;
        }
    }

    @Override
    public long sizeInBytes() {
        return 8L
                + itemName.length()
                + 1L
                + description.length()
                + 1L
                + 8L
                + 8L
                + 8L
                + 8L
                + 8L
                + 8L
                + extra.length()
                + 1L;
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
        Auction auction = (Auction) o;
        return id == auction.id
                && initialBid == auction.initialBid
                && reserve == auction.reserve
                && Objects.equal(ts, auction.ts)
                && Objects.equal(expires, auction.expires)
                && seller == auction.seller
                && category == auction.category
                && Objects.equal(itemName, auction.itemName)
                && Objects.equal(description, auction.description)
                && Objects.equal(extra, auction.extra);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                id, itemName, description, initialBid, reserve, ts, expires, seller, category, extra);
    }

}
