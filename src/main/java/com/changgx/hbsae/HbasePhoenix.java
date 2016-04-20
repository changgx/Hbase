package com.changgx.hbsae;

import org.apache.phoenix.jdbc.PhoenixConnection;

import java.sql.*;

/**
 * Created by liu on 2016/4/15.
 */
public class HbasePhoenix {
    public static Connection getConnection() {
        PhoenixConnection connection = null;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixConnection");
            connection = (PhoenixConnection) DriverManager.getConnection("jdbc:phoenix:172.16.12.74:2181");

        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static void createTable() throws SQLException {
        String sql = "create table liujian(rowkey varchar primary key,name varchar,age integer)";
        Connection connection = getConnection();
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.executeUpdate();
        if (connection != null) {
            connection.close();
        }
    }

    public static void insert() {
        Connection connection=getConnection();
        for(int i=0;i<10;i++){
            String sql = "upsert into LIUJIAN values('rk"+i+"','changgx"+i+"',24)";
            try {
                PreparedStatement preparedStatement=connection.prepareStatement(sql);
                System.out.println(preparedStatement.execute());
//                使用phoenix的jdbc api的upsert语句插入记录，然后用select查询始终查询不到记录，最后发现是因为在插入完毕之后没有做commit().

                connection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }
    public static  void select(){
        Connection connection=getConnection();
        String sql="select * from liujian";
        PreparedStatement ps=null;
        ResultSet resultSet=null;
        try {
             ps=connection.prepareStatement(sql);
             resultSet=ps.executeQuery();
            while(resultSet.next()){
                System.out.println("rowkey:"+resultSet.getString("rowkey")+"  name:"+resultSet.getString("name")+"   age"+resultSet.getString("age"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if(resultSet!=null){
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(ps!=null){
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(connection!=null){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) throws SQLException {

        //createTable();
        //insert();
        select();
    }
}
