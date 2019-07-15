package org.apache.flink.benchmark.nexmark.model;

public class Category {

    public Category(long id){
        this.id = id;
    }

    long id;

    public static String[] getFieldNames(){
        return new String[]{"id"};
    }

}
