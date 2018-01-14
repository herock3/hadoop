package cn.zyx.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.*;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class HDFSAPI {
    FileSystem fs=null;
    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException{
        //此处的端口一定要按照“fs.default.name”修改，若显示windows下ip和linux下不在同一网段，则关闭windows下的虚拟机之内的可能导致ip错误的软件
        fs = FileSystem.get(new URI("hdfs://192.168.1.51:8020"),new Configuration(),"hadoop");
    }
    @Test
    public void uploadFile() throws IOException {
        fs.copyFromLocalFile(new Path("F:\\test\\test2.txt"),new Path("/test"));
    }
    @Test
    public void downloadFile() throws IOException {
        fs.copyToLocalFile(true,new Path("/test/test2.txt"),new Path("F:\\test"));
    }

    @Test
    public void testLs() throws IOException {
        RemoteIterator<LocatedFileStatus> lsr = fs.listFiles(new Path("/test"), true);
        while(lsr.hasNext()) {
            LocatedFileStatus fileStatus = lsr.next();
            System.out.println("时间："+fileStatus.getAccessTime());
            System.out.println("blocksize: " +fileStatus.getBlockSize());
            System.out.println("owner: " +fileStatus.getOwner());
            System.out.println("Replication: " +fileStatus.getReplication());
            System.out.println("Permission: " +fileStatus.getPermission());
            System.out.println("Name: " +fileStatus.getPath().getName());
            System.out.println("------------------");
             BlockLocation[] blocks =fileStatus.getBlockLocations();
             for (BlockLocation bl:blocks){
                 System.out.println("所在主机："+bl.getHosts());
                 System.out.println("偏移量："+bl.getOffset());
             }
        }
    }

//    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
//        HDFSAPI api=new HDFSAPI();
//        api.init();
//        api.uploadFile();
//    }

}