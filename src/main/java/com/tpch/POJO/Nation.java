package com.tpch.POJO;

import java.util.ArrayList;
import java.util.List;

public class Nation implements SQLIterator {
    public Boolean update;
    public int nationKey;
    public String name;
    public int regionKey;
    public String comment;
    public String tag;

    public Nation() {}

    public Nation(Boolean update, int nationKey, String name, int regionKey, String comment, String tag) {
        this.update = update;
        this.nationKey = nationKey;
        this.name = name;
        this.regionKey = regionKey;
        this.comment = comment;
        this.tag = tag;
    }

    public Nation(String[] fields) {
        if (fields.length < 6) {
            return;
        }
        this.update = fields[0].equals("+");
        this.nationKey = Integer.parseInt(fields[1]);
        this.name = fields[2];
        this.regionKey = Integer.parseInt(fields[3]);
        this.comment = fields[4];
        this.tag = fields[5];
    }

    @Override
    public String[] getKeys() {
        return new String[] {
                "N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT", "tag"
        };
    }

    @Override
    public Object[] getValues() {
        List<Object> values = new ArrayList<>();
        values.add(this.nationKey);
        values.add(this.name);
        values.add(this.regionKey);
        values.add(this.comment);
        values.add(this.tag);
        return values.toArray();
    }
}
