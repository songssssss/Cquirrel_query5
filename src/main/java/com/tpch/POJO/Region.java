package com.tpch.POJO;
import java.util.ArrayList;
import java.util.List;

public class Region implements SQLIterator {
    public Boolean update;
    public int regionKey;
    public String name;
    public String comment;
    public String tag;

    public Region() {}

    public Region(Boolean update, String name, int regionKey, String comment, String tag) {
        this.update = update;
        this.name = name;
        this.regionKey = regionKey;
        this.comment = comment;
        this.tag = tag;
    }

    public Region(String[] fields) {
        if (fields.length < 5) {
            return;
        }
        this.update = fields[0].equals("+");
        this.regionKey = Integer.parseInt(fields[1]);
        this.name = fields[2];
        this.comment = fields[3];
        this.tag = fields[4];
    }

    @Override
    public String[] getKeys() {
        return new String[] {
                "R_NAME", "R_REGIONKEY", "R_COMMENT", "tag"
        };
    }

    @Override
    public Object[] getValues() {
        List<Object> values = new ArrayList<>();
        values.add(this.name);
        values.add(this.regionKey);
        values.add(this.comment);
        values.add(this.tag);
        return values.toArray();
    }
}

