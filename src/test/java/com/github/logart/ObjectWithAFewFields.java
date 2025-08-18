package com.github.logart;

import java.util.Map;

public class ObjectWithAFewFields {
    private String regularField;
    private Map<String, String> attributes;

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public String getRegularField() {
        return regularField;
    }

    public void setRegularField(String regularField) {
        this.regularField = regularField;
    }

    public Map<String, Object> getNonNullableMap() {
        return Map.of("key", "value");
    }
}
