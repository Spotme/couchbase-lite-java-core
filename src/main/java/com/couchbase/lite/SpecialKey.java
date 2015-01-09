package com.couchbase.lite;

public class SpecialKey {
    private String text;

    public SpecialKey(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }
}
