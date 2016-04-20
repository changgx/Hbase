package com.changgx.hbsae;


import org.apache.commons.configuration.ConfigurationFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by liu on 2016/4/13.
 */

public class HbaseCRUD {
    public static void main(String[] args) throws IOException {
//        createTable();
//        putTable();
        putListTable();
        scanTable();
//        getTable();
    }

    public static void putTable() {
        Configuration conf = HBaseConfiguration.create();
        HTable hTable = null;
        try {
            hTable = new HTable(conf, Bytes.toBytes("liu"));
            Put put = new Put(Bytes.toBytes("rk001"));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("changgx001"));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes("24"));
            put.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("sex"), Bytes.toBytes("man"));
            hTable.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (hTable != null) {
                try {
                    hTable.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void createTable() {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin hBaseAdmin = null;
        try {
            hBaseAdmin = new HBaseAdmin(conf);
            boolean b = hBaseAdmin.tableExists(Bytes.toBytes("liu"));
            if (b) {
                hBaseAdmin.disableTable(Bytes.toBytes("liu"));
                hBaseAdmin.deleteTable(Bytes.toBytes("liu"));
            }
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("liu"));
            hTableDescriptor.addFamily(new HColumnDescriptor(Bytes.toBytes("cf1")));
            hTableDescriptor.addFamily(new HColumnDescriptor(Bytes.toBytes("cf2")));
            hBaseAdmin.createTable(hTableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (hBaseAdmin != null) {
                try {
                    hBaseAdmin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    public static void getTable() {
        Configuration configuration = HBaseConfiguration.create();
        HTable hTable = null;
        try {
            hTable = new HTable(configuration, Bytes.toBytes("liu"));
            Get get = new Get(Bytes.toBytes("rk001"));
            //get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
            Result result = hTable.get(get);
            System.out.println(Bytes.toString(result.getRow()) + "  " + Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name"))));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (hTable != null) {
                try {
                    hTable.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void putListTable() {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = null;
        Table table = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf(Bytes.toBytes("liu")));
            List<Put> list = new ArrayList<Put>();
            for (int i = 0; i < 1000; i++) {
                Put put = new Put(Bytes.toBytes("rk00" + i));
                put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("changgx" + i));
                put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(22));
                list.add(put);
            }
            table.put(list);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public static void scanTable() {
        Configuration conf = HBaseConfiguration.create();
        //HBaseConfiguration.create() 会默认加载 classpath下的hbase-site.xml和hbase-default.xml文件
        //代码配置优先于属性文件配置
//        conf.set("hbase.zookeeper.property.clientPort","2181" );
//        conf.set("hbase.zookeeper.quorum", "172.16.12.71");
        HTable htable = null;
        ResultScanner resultScanner = null;
        try {
            htable = new HTable(conf, Bytes.toBytes("liu"));
            Scan scan = new Scan();
            resultScanner = htable.getScanner(scan);
            while (true) {
                org.apache.hadoop.hbase.client.Result result = resultScanner.next();
                if (result == null) {
                    break;
                }
                System.out.println(Bytes.toString(result.getRow()) + "  "
                        + Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name"))) + "  "
                        + Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("age"))) + "  "
                        + Bytes.toString(result.getValue(Bytes.toBytes("cf2"), Bytes.toBytes("sex"))));
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (resultScanner != null) {
                resultScanner.close();
            }
            if (resultScanner != null) {
                try {
                    htable.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }


    }
}
