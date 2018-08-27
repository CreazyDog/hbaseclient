package com.hbaseclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseClient1 {
    public static Configuration conf;
    public static Connection connection;
    public static Admin admin;
    public static void main(String[] args) throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.master", "10.75.57.25:16000");
        conf.set("zookeeper.znode.parent", "/weibo_search");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "10.75.57.29:2181,10.75.57.29:2181,10.75.57.30:2181,10.75.57.31:2181,10.75.57.32:2181,10.75.57.33:2181,10.75.57.34:2181,10.75.57.35:2181");
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf("table1"));
        table.addFamily(new HColumnDescriptor("group1")); //创建表时至少加入一个列组
        if(admin.tableExists(table.getTableName())){
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }
}
