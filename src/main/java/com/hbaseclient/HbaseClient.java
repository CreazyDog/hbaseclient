package com.hbaseclient;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

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

    //如果表已经存在先删除表然后再创建
    public static void createTable(String tableNameStr, String[] familyNames) {
        System.out.printf("开始创建表");
        try {
            TableName tableName = TableName.valueOf(tableNameStr);
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.printf(tableName + "已经存在，正在删除");
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            if (null != familyNames && familyNames.length > 0) {
                for (String familyname : familyNames) {
                    tableDescriptor.addFamily(new HColumnDescriptor(familyname));
                }
            }
            admin.createTable(tableDescriptor);
            System.out.printf("创建表结束");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /*
     插入数据
     */
    public static void insertData(String tableName, String rowId, String familyName, String qualifier, String value) {
        try {
            System.out.println("开始插入数据");
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(rowId.getBytes());
            put.addColumn(familyName.getBytes(), qualifier.getBytes(), value.getBytes());
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("插入数据结束");
    }

    //删除单行
    public static void deleteRow(String tableName, String rowkey) {
        try {
            System.out.printf("开始删除行" + rowkey);
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete d1 = new Delete(rowkey.getBytes());
            table.delete(d1);
            System.out.printf("删除行" + rowkey + "成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
     删除单个行
     */
    public static void dropTable(String tableNameStr) {
        try {
            System.out.printf("开始删除表");
            TableName tableName = TableName.valueOf(tableNameStr);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            admin.close();
            System.out.printf("删除表结束");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
    查询所有数据
     */
    public static void queryAll(String tableName) {
        System.out.printf("开始查询所有数据");
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            ResultScanner rs = table.getScanner(new Scan());
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (Cell keyValue : r.rawCells()) {
                    System.out.println("列:" + new String(CellUtil.cloneFamily(keyValue))
                            + ":" + new String(CellUtil.cloneQualifier(keyValue)) + "===值:"
                            + new String(CellUtil.cloneValue(keyValue)));
                }
            }
            rs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.printf("查询所有数据结束");
    }

    /**
     * 根据rowid查询
     *
     * @throws IOException
     */
    public static void queryByRowId(String tableName, String rowId) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get scan = new Get(rowId.getBytes());
            Result r = table.get(scan);
            System.out.println("获得rowkey:" + new String(r.getRow()));
            for (Cell keyValue : r.rawCells()) {
                System.out.println("列:" + new String(CellUtil.cloneFamily(keyValue))
                        + ":" + new String(CellUtil.cloneQualifier(keyValue)) + "===值:"
                        + new String(CellUtil.cloneValue(keyValue)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     单条件过滤查询
     */
    public static void queryByCondition(String tableName, String familyName, String qualifier, String value) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName),
                    Bytes.toBytes(qualifier),
                    CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes(value)
            );
            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (Cell keyValue : r.rawCells()) {
                    System.out.println("列:" + new String(CellUtil.cloneFamily(keyValue))
                            + ":" + new String(CellUtil.cloneQualifier(keyValue)) + "===值:"
                            + new String(CellUtil.cloneValue(keyValue)));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /*
    多条件过滤查询
     */
    public static void queryByConditions(String tableName, String familyNames[], String qualifiers[], String values[]) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            List<Filter> filters = new ArrayList<Filter>();
            if (null != familyNames && familyNames.length > 0) {
                int i = 0;
                for (String familyName : familyNames) {
                    Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName),
                            Bytes.toBytes(qualifiers[i]),
                            CompareFilter.CompareOp.EQUAL,
                            Bytes.toBytes(values[i])
                    );
                    filters.add(filter);
                    i++;
                }
            }
            FilterList filterList = new FilterList(filters);
            Scan scan = new Scan();
            scan.setFilter(filterList);
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (Cell keyValue : r.rawCells()) {
                    System.out.println("列:" + new String(CellUtil.cloneFamily(keyValue))
                            + ":" + new String(CellUtil.cloneQualifier(keyValue)) + "===值:"
                            + new String(CellUtil.cloneValue(keyValue)));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
     }
     public  static  void  getTableDesc(String tableName)
     {
         try {
             Set<String> columnList = new TreeSet<String>();
             Table table = connection.getTable(TableName.valueOf(tableName));
             Scan scan=new Scan();
             scan.setFilter(new PageFilter(100));
             ResultScanner results = table.getScanner(scan);
             for (Result r : results) {
                 for (Cell keyValue : r.rawCells()) {
                     columnList.add(
                             new String(CellUtil.cloneFamily(keyValue)) + "\t" +
                                     new String(CellUtil.cloneQualifier(keyValue)));
                 }
             }
             for(String s:columnList)
             {
                 System.out.println(s);
             }
         }
         catch (Exception e)
         {
             e.printStackTrace();
         }
     }




    public static void main(String[] args) throws IOException {
        //StringUtils.trimToNull("aaa");
        getTableDesc("weibo");
        /*String[] familyNames = {"gender", "grade"};
        //createTable("score", familyNames);
       *//* insertData("score", "1", "grade", "chines", "80");
        insertData("score", "1", "grade", "math", "70");
        insertData("score", "1", "grade", "english", "90");
        insertData("score", "2", "grade", "chinese", "90");
        insertData("score", "2", "grade", "math", "70");
        insertData("score", "2", "grade", "english", "60");
       *//*
        // queryAll("score");
        //  deleteRow("score", "2");
        //dropTable("score");
        //queryByRowId("score", "1");
        //queryByCondition("score","gender","male","1");
        String qualifiers[] = {"male", "math"};
        String values[] = {"1", "70"};
        queryByConditions("score", familyNames, qualifiers, values);*/
        //queryByCondition("weibo","cf","7","10908141364");
//        queryByCondition("weibo","cf","7","10908141364");
        //queryByRowId("weibo", "0000000b53d65261a47d");
        //根据weibo的mid查询微博数据的方法有2种
        //方法1：使用hbase的过滤器Filter，在数据量很小的时候没有问题，数据量上去之后查询缓慢
        //queryByCondition("weibo", "cf", "7", "109082338759");
        //方法2：微博表weibo的rowKey是根据微博的mid通过一些列算法转换实现的，可以通过转化获取微博表weibo
        //的rowKey，然后通过rowKey来查询单条微博即可
        //String rowKey = HashUtils.getRowKey("109082338759", 3);
        //mid：109082338759 rowKey:00000000001965d065c7 url:http://t.sina.com.cn/1356313647/2PW9OpV
        //System.out.printf(rowKey);
        //queryByRowId("weibo", rowKey);
        //根据url来查询微博，也是两种方式，但还是需要通过算法将url转换成rowKey来进行查
/*
        System.out.println(UrlUtils.url2Mid("" +
                "https://weibo.com/1742566624/GwK0XgB91?ref=home&rid=9_0_8_3076372047766889666_0_0&type=comment#_rnd1535449374216"
        ));
      */
        //System.out.println(HashUtils.getRowKey("4280110032735399"));
        //System.out.println(HashUtils.getRowKey("4278033793956035", 3));
       /* String url="http://t.sina.com.cn/1650479997/zaAVGEtY9";
        String mid=UrlUtils.url2Mid(url);
        String rowKey=HashUtils.getRowKey(mid,3);*/
        //queryByRowId("weibo","73a0000f32dcb0d5fb64");
        StringUtils.trimToNull("a");
    }
}
