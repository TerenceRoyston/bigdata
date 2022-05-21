package com.xubowen.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.IOException;

/**
 * @author XuBowen
 * @date 2021/12/27 22:52
 */
public class HDFSAPI {

    // 创建客户端对象
    public static FileSystem createFileSystem() throws IOException {
        Configuration conf = new Configuration();
        return FileSystem.get(conf);

    }


    // 创建目录
    @Test
    public void mkHDFSDir() throws IOException {
        FileSystem fs = createFileSystem();
        System.out.println(fs.getClass().getName());
        fs.mkdirs(new Path("/tmp/hdfs_api"));
        fs.close();
    }

    // 上传文件
    @Test
    public void uploadFile() throws IOException {
        FileSystem fs = createFileSystem();
        fs.copyFromLocalFile(false, true, new Path("G:/Python.jpg"), new Path("/tmp/hdfs_api"));
    }

    // 下载文件
    @Test
    public void downloadFile() throws IOException {
        FileSystem fs = createFileSystem();
        fs.copyToLocalFile(false, new Path("/tmp/hdfs_api/Python.jpg"), new Path("H:/"), false);
    }

    // 删除文件
    @Test
    public void deleteFile() throws IOException {
        FileSystem fs = createFileSystem();
        fs.delete(new Path("/tmp/hdfs_api/Python.jpg"), false);
    }

    // 重命名
    @Test
    public void renameFile() throws IOException {
        FileSystem fs = createFileSystem();
        fs.rename(new Path("/tmp/hdfs_api/Python.jpg"), new Path("/tmp/hdfs_api/Python666.jpg"));
    }

    // 判断当前路径是否存在
    @Test
    public void isExists() throws IOException {
        FileSystem fs = createFileSystem();
        boolean exists = fs.exists(new Path("/tmp/hdfs_api/Python.jpg"));
        System.out.println(exists);

    }

    // 判断路径是目录还是文件 fileStatus封装了更多丰富的内容
    @Test
    public void isFileOrPath() throws IOException {
        FileSystem fs = createFileSystem();
        boolean fsDirectory = fs.isDirectory(new Path("/tmp/hdfs_api/Python.jpg"));
        boolean fsFile = fs.isFile(new Path("/tmp/hdfs_api/Python.jpg"));
        System.out.println(fsDirectory);
        System.out.println(fsFile);
    }

    // 获取文件的块信息
    // LocatedFileStatus是FileStatus的子类 除了文件属性还有块的位置信息
    @Test
    public void getBlockInfo() throws IOException {
        FileSystem fs = createFileSystem();
        RemoteIterator<LocatedFileStatus> status = fs.listLocatedStatus(new Path("/tmp/hdfs_api/CDH5.1.14全网最完整安装_高清 1080P.mp4"));

        while (status.hasNext()) {
            LocatedFileStatus locatedFileStatus = status.next();
            System.out.println(locatedFileStatus.getOwner());

            BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                System.out.println(blockLocation);
                System.out.println("------------");
            }
        }
    }


    // 自定义上传  不会写就参考源码实现
    // 自定义上传下载本质依然是IO流
    @Test
    public void customUploadFile() throws IOException {

        // 获取两个文件系统
        FileSystem hdfsFS = createFileSystem();
        Configuration localConf = new Configuration();
        localConf.set("fs.defaultFS", "file:///");
        FileSystem localFS = FileSystem.get(localConf);

        // 获取两个path对象
        Path src = new Path("H:/李海波课程/CDH5.1.14全网最完整安装/CDH5.1.14全网最完整安装_高清 1080P.mp4");
        Path dest = new Path("/tmp/hdfs_api/CDH_10M.mp4");

        // 创建输入流与输出流
        FSDataInputStream inputStream = localFS.open(src);
        FSDataOutputStream outputStream = hdfsFS.create(dest, true);

        // 流的对拷
        byte[] buffer = new byte[1024];
        for (int i = 0; i < 1024 * 10; i++) {
            inputStream.read(buffer);
            outputStream.write(buffer);
        }

        // 关流
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(outputStream);

    }

    // 自定义下载
    @Test
    public void customDownloadFile() throws IOException{
        // 获取两个文件系统
        FileSystem hdfsFS = createFileSystem();
        Configuration localConf = new Configuration();
        localConf.set("fs.defaultFS", "file:///");
        FileSystem localFS = FileSystem.get(localConf);

        // 获取两个path对象
        Path src = new Path("/tmp/hdfs_api/CDH_10M.mp4");
        Path dest = new Path("G:/CDH_5M.mp4");

        // 创建输入流与输出流
        FSDataInputStream inputStream = hdfsFS.open(src);
        FSDataOutputStream outputStream = localFS.create(dest, true);

        // 流的对拷
        byte[] buffer = new byte[1024];
        for (int i = 0; i < 1024 * 5; i++) {
            inputStream.read(buffer);
            outputStream.write(buffer);
        }

        // 关流
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(outputStream);

        // 还可以使用seek方法从文件中间位置开始下载

    }


}
