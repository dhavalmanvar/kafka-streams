package com.github.dhavalmanvar.kafka;

public enum DataType {

    STRING("string"),
    INT("int"),
    LONG("long"),
    FLOAT("float"),
    custom("custom");

    private String type;

    DataType(String type) {
        this.type = type;
    }

    public String getType(){
        return this.type;
    }
}
