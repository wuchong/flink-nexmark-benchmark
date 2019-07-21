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
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Objects;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.benchmark.nexmark.NexmarkUtils;
import org.apache.flink.table.api.Types;

import java.io.Serializable;
import java.sql.Timestamp;


/**
 * A person either creating an auction or making a bid.
 */
@DefaultSchema(JavaFieldSchema.class)
public class Person implements KnownSize, Serializable {

    /**
     * Id of person.
     */
    @JsonProperty
    public long id; // primary key

    /**
     * Extra person properties.
     */
    @JsonProperty
    public String name;

    @JsonProperty
    public String emailAddress;

    @JsonProperty
    public String creditCard;

    @JsonProperty
    public String city;

    @JsonProperty
    public String state;

    @JsonProperty
    public Timestamp ts;

    /**
     * Additional arbitrary payload for performance testing.
     */
    @JsonProperty
    public String extra;

    // For Avro only.
    @SuppressWarnings("unused")
    public Person() {
        id = 0;
        name = null;
        emailAddress = null;
        creditCard = null;
        city = null;
        state = null;
        ts = new Timestamp(0);
        extra = null;
    }

    public Person(
            long id,
            String name,
            String emailAddress,
            String creditCard,
            String city,
            String state,
            Timestamp timestamp,
            String extra) {
        this.id = id;
        this.name = name;
        this.emailAddress = emailAddress;
        this.creditCard = creditCard;
        this.city = city;
        this.state = state;
        this.ts = timestamp;
        this.extra = extra;
    }

    public static String[] getFieldNames() {
        return new String[]{"id", "name", "emailAddress", "creditCard",
                "city", "state", "ts", "extra"};
    }

    public static TypeInformation[] getFieldTypes() {
        return new TypeInformation[]{Types.LONG(), Types.STRING(), Types.STRING(), Types.STRING(),
                Types.STRING(), Types.STRING(),  Types.LONG(), Types.STRING()};
    }

    /**
     * Return a copy of person which capture the given annotation. (Used for debugging).
     */
    public Person withAnnotation(String annotation) {
        return new Person(
                id, name, emailAddress, creditCard, city, state, ts, annotation + ": " + extra);
    }

    /**
     * Does person have {@code annotation}? (Used for debugging.)
     */
    public boolean hasAnnotation(String annotation) {
        return extra.startsWith(annotation + ": ");
    }

    /**
     * Remove {@code annotation} from person. (Used for debugging.)
     */
    public Person withoutAnnotation(String annotation) {
        if (hasAnnotation(annotation)) {
            return new Person(
                    id,
                    name,
                    emailAddress,
                    creditCard,
                    city,
                    state,
                    ts,
                    extra.substring(annotation.length() + 2));
        } else {
            return this;
        }
    }

    @Override
    public long sizeInBytes() {
        return 8L
                + name.length()
                + 1L
                + emailAddress.length()
                + 1L
                + creditCard.length()
                + 1L
                + city.length()
                + 1L
                + state.length()
                + 8L
                + 1L
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
        Person person = (Person) o;
        return id == person.id
                && Objects.equal(ts, person.ts)
                && Objects.equal(name, person.name)
                && Objects.equal(emailAddress, person.emailAddress)
                && Objects.equal(creditCard, person.creditCard)
                && Objects.equal(city, person.city)
                && Objects.equal(state, person.state)
                && Objects.equal(extra, person.extra);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, name, emailAddress, creditCard, city, state, ts, extra);
    }
}
