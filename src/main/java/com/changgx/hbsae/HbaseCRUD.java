package com.changgx.hbsae;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by liu on 2016/4/13.
 */

public class HbaseCRUD {
    public static void main(String[] args) throws IOException {
//        createTable();
//        putTable();
        scanTable();

    }
    public static void putTable(){
        Configuration conf=HBaseConfiguration.create();
        HTable hTable=null;
        try {
            hTable=new HTable(conf,Bytes.toBytes("liu"));
            Put put=new Put(Bytes.toBytes("rk001"));
            put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("name"),Bytes.toBytes("changgx001"));
            put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("age"),Bytes.toBytes("24"));
            put.addColumn(Bytes.toBytes("cf2"),Bytes.toBytes("sex"),Bytes.toBytes("man"));
            hTable.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(hTable!=null){
                try {
                    hTable.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    public static void createTable(){
        Configuration conf=HBaseConfiguration.create();
        HBaseAdmin hBaseAdmin=null;
        try {
            hBaseAdmin=new HBaseAdmin(conf);
            boolean b= hBaseAdmin.tableExists(Bytes.toBytes("liu"));
            if(b){
                hBaseAdmin.disableTable(Bytes.toBytes("liu"));
                hBaseAdmin.deleteTable(Bytes.toBytes("liu"));
            }
            HTableDescriptor hTableDescriptor=new HTableDescriptor(TableName.valueOf("liu"));
            hTableDescriptor.addFamily(new HColumnDescriptor(Bytes.toBytes("cf1")));
            hTableDescriptor.addFamily(new HColumnDescriptor(Bytes.toBytes("cf2")));
            hBaseAdmin.createTable(hTableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(hBaseAdmin!=null){
                try {
                    hBaseAdmin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }
    public static void getTable(){
        Configuration configuration=HBaseConfiguration.create();
    }

    public static void scanTable() {
        Configuration conf = HBaseConfiguration.create();

//          Java客户端使用的配置信息是被映射在一个HBaseConfiguration 实例中.
//          HBaseConfiguration有一个工厂方法, HBaseConfiguration.create();
//          运行这个方法的时候,他会去CLASSPATH,下找hbase-site.xml，读他发现的第一个配置文件的内容。
        //应该是代码配置的优先级高于配置文件的
//        conf.set("hbase.zookeeper.property.clientPort","2181" );
//        conf.set("hbase.zookeeper.quorum", "172.16.12.71");
        HTable htable = null;
        ResultScanner resultScanner=null;
        try {
            htable=new HTable(conf, Bytes.toBytes("liu"));
            Scan scan = new Scan();
           resultScanner = htable.getScanner(scan);
            while (true) {
                org.apache.hadoop.hbase.client.Result result = resultScanner.next();
                if (result == null) {
                    break;
                }
                System.out.println(Bytes.toString(result.getRow())+"  "
                        +Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name")))+"  "
                        +Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("age")))+"  "
                        +Bytes.toString(result.getValue(Bytes.toBytes("cf2"), Bytes.toBytes("sex"))));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(resultScanner!=null){
                resultScanner.close();
            }
            if(resultScanner!=null){
                try {
                    htable.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }


    }
}
