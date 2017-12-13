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
        fs = FileSystem.get(new URI("hdfs://192.168.47.100:9000"),new Configuration(),"hadoop");
    }
    @Test
    public void uploadFile() throws IOException {
        fs.copyFromLocalFile(new Path("c:\\test2.txt"),new Path("/path"));
    }
    @Test
    public void downloadFile() throws IOException {
        fs.copyToLocalFile(true,new Path("/test.txt"),new Path("d:/"));
    }

    @Test
    public void testLs() throws IOException {
        RemoteIterator<LocatedFileStatus> lsr = fs.listFiles(new Path("/"), true);
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