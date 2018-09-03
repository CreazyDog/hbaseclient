package com.hbaseclient;

import org.apache.hadoop.hbase.util.Bytes;

public class Test {
    public static void main(String[] args) {
        System.out.printf(StringConvertor.bytesToHexString(Bytes.toBytes(Long.parseLong("4278048091667300"))));
    }
}
