package com.changgx.hbsae;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;

import java.io.IOException;

/**
 * Created by liu on 2016/4/14.
 */
public class HbaseRowFilter {
    private static String tableName="liu";
    private static Configuration config= HBaseConfiguration.create();

    public static void main(String[] args) {
        RowFilterDemo();
    }
    public static void RowFilterDemo() {
        HTable hTable=null;
        try {
            hTable=new HTable(config,tableName);
            Scan scan=new Scan();
            System.out.println("小于rk003的");
            Filter filter=new RowFilter(CompareFilter.CompareOp.LESS,
                    new BinaryComparator("rk003".getBytes()));
            scan.setFilter(filter);
            ResultScanner resultScanner=hTable.getScanner(scan);
            for(Result result:resultScanner){
                System.out.println(result);
            }
            resultScanner.close();


            System.out.println("rk小于等于rk003");
            Filter filter1=new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
                    new BinaryComparator("rk003".getBytes()));
            scan.setFilter(filter1);
            ResultScanner resultScanner1=hTable.getScanner(scan);
            for(Result result:resultScanner1){
                System.out.println(result);
            }
            resultScanner1.close();

            System.out.println("正则表达是5");
            Filter filter2=new RowFilter(CompareFilter.CompareOp.EQUAL,
                    new RegexStringComparator(".*5$"));
            scan.setFilter(filter2);
            ResultScanner resultScanner2=hTable.getScanner(scan);
            for(Result result:resultScanner2){
                System.out.println(result);
            }


            System.out.println("包含004");
            Filter filter3=new RowFilter(CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator("004"));
            scan.setFilter(filter3);
            ResultScanner resultScanner3=hTable.getScanner(scan);
            for(Result result:resultScanner3){
                System.out.println(result);
            }

            System.out.println("以rowkey开始的");
            Filter filter4=new RowFilter(CompareFilter.CompareOp.EQUAL,
                    new BinaryPrefixComparator("rowkey".getBytes()));

            ResultScanner resultScanner4=hTable.getScanner(scan);
            for(Result result:resultScanner4){
                System.out.println(result);
            }


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
}
