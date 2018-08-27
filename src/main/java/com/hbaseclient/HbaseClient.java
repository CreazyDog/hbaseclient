package com.hbaseclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class HbaseClient {
    public static Configuration conf;
    public static Connection connection;
    public static Admin admin;

    static {
        try {
            conf = HBaseConfiguration.create();
            conf.set("hbase.master", "10.75.57.25:16000");
            conf.set("zookeeper.znode.parent", "/weibo_search");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("hbase.zookeeper.quorum", "10.75.57.29:2181,10.75.57.29:2181,10.75.57.30:2181,10.75.57.31:2181,10.75.57.32:2181,10.75.57.33:2181,10.75.57.34:2181,10.75.57.35:2181");
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static  void createTable(String tableNameStr,String [] familyNames)
    {
        System.out.printf("开始创建表");
        try {
            TableName tableName=TableName.valueOf(tableNameStr);
            if(admin.tableExists(tableName))
            {

            }

        }
        catch (IOException e)
        {
            e.printStackTrace();
        }


    }


    public static void main(String[] args) throws IOException {
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf("table1"));
        table.addFamily(new HColumnDescriptor("group1")); //创建表时至少加入一个列组
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }
}
