package com.xubowen.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author XuBowen
 * @date 2021/12/4 16:07
 */
public class HBaseConnection {
    public static void main(String[] args) throws IOException {
        // 创建配置对象
        Configuration conf = HBaseConfiguration.create();
        // 获取HBase连接
        Connection connection = ConnectionFactory.createConnection(conf);
        System.out.println(connection);
        // 获取操作对象
        // new HBaseAdmin(connection);   方法过时，不推荐
        Admin admin = connection.getAdmin();

        // 操作数据库

        // 判断命名空间是否存在
        NamespaceDescriptor namespaceDescriptor = admin.getNamespaceDescriptor("default");
        System.out.println(namespaceDescriptor);


        // 判断表是否存在
        TableName fruit = TableName.valueOf("fruit");
        boolean isExists = admin.tableExists(fruit);
        System.out.println(isExists);


    }

    public static Connection getHBaseConnection() throws IOException {
        // 创建配置对象
        Configuration conf = HBaseConfiguration.create();
        // 获取HBase连接
        Connection connection = ConnectionFactory.createConnection(conf);
        System.out.println(connection);
        return connection;
    }



    /**
     * 创建表
     */
    @Test
    public void createHBaseTable() throws IOException {
        // 创建配置对象
        Configuration conf = HBaseConfiguration.create();
        // 获取HBase连接
        Connection connection = ConnectionFactory.createConnection(conf);
        // 获取操作对象
        Admin admin = connection.getAdmin();
        TableName zte = TableName.valueOf("zte");

        HTableDescriptor td = new HTableDescriptor(zte);

        // 创建列族描述对象
        HColumnDescriptor cd = new HColumnDescriptor("info");
        td.addFamily(cd);

        // 建表
        admin.createTable(td);
        System.out.println("hbase zte表创建成功！");

    }

    /**
     * 查询表中数据
     * @throws IOException
     */
    @Test
    public void queryTableInfo() throws IOException {
        Connection connection = HBaseConnection.getHBaseConnection();
        TableName fruit = TableName.valueOf("fruit");
        Table table = connection.getTable(fruit);
        // rowkey如果是中文可能会有乱码，要注意解码
        Get get = new Get(Bytes.toBytes("1002"));
        Result result = table.get(get);
        System.out.println(result.isEmpty());
        for (Cell cell : result.rawCells()) {
            System.out.println("rowkey => "+ Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println("family => "+Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("column => "+Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("value => "+Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    @Test
    public void addTableData() throws IOException {
        Connection connection = HBaseConnection.getHBaseConnection();
        TableName zte = TableName.valueOf("zte");
        Table table = connection.getTable(zte);
        Put put = new Put(Bytes.toBytes("1000"));
        String family = "info";
        String column = "name";
        String value="jack";
        put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(value));
        table.put(put);
        System.out.println("成功新增数据！");
    }

    /**
     * 删除表
     */
    @Test
    public void deleteHBaseTable() throws IOException {
        // 创建配置对象
        Configuration conf = HBaseConfiguration.create();
        // 获取HBase连接
        Connection connection = ConnectionFactory.createConnection(conf);
        // 获取操作对象
        Admin admin = connection.getAdmin();
        TableName zte = TableName.valueOf("zte");
        if (admin.tableExists(zte)){
            // 先禁用，再删除
            admin.disableTable(zte);
            admin.deleteTable(zte);
        }
    }

    /**
     * 删除表中数据
     * @throws IOException
     */
    @Test
    public void deleteTableData() throws IOException {
        Connection connection = HBaseConnection.getHBaseConnection();
        TableName zte = TableName.valueOf("zte");
        Table table = connection.getTable(zte);
        Delete delete = new Delete(Bytes.toBytes("1002"));
        table.delete(delete);
        System.out.println("成功删除数据！");
    }

    /**
     * 批量查询表中数据
     * @throws IOException
     */
    @Test
    public void scanTableData() throws IOException {
        Connection connection = HBaseConnection.getHBaseConnection();
        TableName zte = TableName.valueOf("zte");
        Table table = connection.getTable(zte);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("rowkey => "+ Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("family => "+Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("column => "+Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("value => "+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

}
