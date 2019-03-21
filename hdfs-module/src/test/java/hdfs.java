import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class hdfs {

    @Test
    public void testmkDir() {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        // configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
        // FileSystem fs = FileSystem.get(configuration);

        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI("hdfs://122.112.245.103:9000"), configuration, "root");
            // 2 创建目录
            fs.mkdirs(new Path("/0319/lyy/banzhang"));

            // 3 关闭资源
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testUpload() {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI("hdfs://122.112.245.103:9000"), configuration, "root");
            // 2 上传文件
            fs.copyFromLocalFile(new Path("e:/banzhang.txt"), new Path("/banzhang.txt"));

            // 3 关闭资源
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        System.out.println("over");
    }

    @Test
    public void testDownLoad() {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI("hdfs://122.112.245.103:9000"), configuration, "root");
            // 2 执行下载操作
            // boolean delSrc 指是否将原文件删除
            // Path src 指要下载的文件路径
            // Path dst 指将文件下载到的路径
            // boolean useRawLocalFileSystem 是否开启文件校验
            fs.copyToLocalFile(false, new Path("/banzhang.txt"), new Path("e:/banhua.txt"), true);

            // 3 关闭资源
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }


    }


    @Test
    public void testDelete() {
// 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI("hdfs://122.112.245.103:9000"), configuration, "root");
            // 2 执行删除
            fs.delete(new Path("/0319/"), true);
            // 3 关闭资源
            fs.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testChangeName() {
// 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI("hdfs://122.112.245.103:9000"), configuration, "root");
            // 2 修改文件名称
            fs.rename(new Path("/banzhang.txt"), new Path("/banhua.txt"));

            // 3 关闭资源
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testFileInfo() throws Exception {
// 1获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://122.112.245.103:9000"), configuration, "root");

        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();

            // 输出详情
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 分组
            System.out.println(status.getGroup());

            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {

                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();

                for (String host : hosts) {
                    System.out.println(host);
                }
            }

            System.out.println("-----------班长的分割线----------");
        }
// 3 关闭资源
        fs.close();

    }


    @Test
    public void testListStatus() throws Exception {
        // 1 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://122.112.245.103:9000"), configuration, "root");

        // 2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {

            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:" + fileStatus.getPath().getName());
            } else {
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }

        // 3 关闭资源
        fs.close();

    }

    @Test
    public void putFileToHDFS() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://122.112.245.103:9000"), configuration, "root");

        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("e:/banzhang.txt"));

        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/banhua1.txt"));

        // 4 流对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();

    }

    @Test
    public void getFileFromHDFS() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://122.112.245.103:9000"), configuration, "root");

        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/banhua1.txt"));

        // 3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/banhua.txt"));

        // 4 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    @Test
    public void readFileSeek1() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://122.112.245.103:9000"), configuration, "root");

        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 3 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part1"));

        // 4 流的拷贝
        byte[] buf = new byte[1024];

        for (int i = 0; i < 1024 * 128; i++) {
            fis.read(buf);
            fos.write(buf);
        }

        // 5关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    /**
     * 在Window命令窗口中进入到目录E:\，然后执行如下命令，对数据进行合并
     * type hadoop-2.7.2.tar.gz.part2 >> hadoop-2.7.2.tar.gz.part1
     * 合并完成后，将hadoop-2.7.2.tar.gz.part1重新命名为hadoop-2.7.2.tar.gz。解压发现该tar包非常完整。
     * @throws Exception
     */
    @Test
    public void readFileSeek2() throws Exception{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://122.112.245.103:9000"), configuration, "root");

        // 2 打开输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 3 定位输入数据位置
        fis.seek(1024*1024*128);

        // 4 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part2"));

        // 5 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 6 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }
}
