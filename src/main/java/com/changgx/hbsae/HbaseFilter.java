package com.changgx.hbsae;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by liu on 2016/4/15.
 */
public class HbaseFilter {
    public static void main(String[] args) {
        selectByFilter();
    }
    public static void selectByFilter(){
        Configuration configuration= HBaseConfiguration.create();
        ResultScanner resultScanner=null;
        Table table=null;
        try {
            Connection connection= ConnectionFactory.createConnection(configuration);
             table=connection.getTable(TableName.valueOf("liu"));
            Scan scan=new Scan();
//            设置过滤器之间的逻辑关系。默认是 且
            FilterList filterList=new FilterList(FilterList.Operator.MUST_PASS_ALL);
            // SubstringComparator是包含关系，与ComparaOp.EQUAL一起是匹配所有包含字符串的
            //与CompareOpen.GREATER 是匹配大于并且包含的字符串的
//            Filter filter=new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,new BinaryComparator(Bytes.toBytes("rk00800")));
//            Filter filter=new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator("671"));
//            filterList.addFilter(filter);
           //值过滤器
            Filter filter=new SingleColumnValueFilter(Bytes.toBytes("cf1"),Bytes.toBytes("name"),
                    CompareFilter.CompareOp.GREATER_OR_EQUAL,new BinaryComparator(Bytes.toBytes("changgx671")));
           // scan.setFilter(filterList);
            scan.setFilter(filter);
             resultScanner=table.getScanner(scan);
            for (Result result=resultScanner.next();result!=null;result=resultScanner.next()){
                System.out.println(Bytes.toString(result.getRow())+"  "+
                        Bytes.toString(result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("name"))));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(resultScanner!=null){
                resultScanner.close();
            }
            if(table!=null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
