package org.apache.flink.benchmark.nexmark.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class Category implements Serializable {

    @JsonProperty
    long id;

    public Category(){
        this.id = 0;
    }

    public Category(long id){
        this.id = id;
    }


    public static String[] getFieldNames(){
        return new String[]{"id"};
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}
