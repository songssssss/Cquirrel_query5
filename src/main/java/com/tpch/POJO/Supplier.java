package com.tpch.POJO;
import java.util.ArrayList;

public class Supplier implements SQLIterator {
    public Boolean update;
    public int suppKey;
    public String name;
    public String address;
    public int nationKey;
    public String phone;
    public float acctbal;
    public String comment;
    public String tag;

    public Supplier() {
    }

    public Supplier(Boolean update, int suppKey, String name, String address, int nationKey, String phone, float acctbal, String comment, String tag) {
        this.update = update;
        this.suppKey = suppKey;
        this.name = name;
        this.address = address;
        this.nationKey = nationKey;
        this.phone = phone;
        this.acctbal = acctbal;
        this.comment = comment;
        this.tag = tag;
    }

    public Supplier(String[] fields) {
        if (fields.length < 9) {
            return;
        }
        this.update = fields[0].equals("+");
        this.suppKey = Integer.parseInt(fields[1]);
        this.name = fields[2];
        this.address = fields[3];
        this.nationKey = Integer.parseInt(fields[4]);
        this.phone = fields[5];
        this.acctbal = Float.parseFloat(fields[6]);
        this.comment = fields[7];
        this.tag = fields[8];
    }

    @Override
    public String[] getKeys() {
        return new String[]{
                "S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY",
                "S_PHONE", "S_ACCTBAL", "S_COMMENT", "tag"
        };
    }

    @Override
    public Object[] getValues() {
        ArrayList<Object> values = new ArrayList<Object>();
        values.add(suppKey);
        values.add(name);
        values.add(address);
        values.add(nationKey);
        values.add(phone);
        values.add(acctbal);
        values.add(comment);
        values.add(tag);
        return values.toArray();
    }
}
