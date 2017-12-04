package cn.zyx.hadoop.hdfs;
/**
 * 流的方式读取HDFS文件，其目的是为了后期的MapReduce运算，流可以被分割成不同的段，然后给不同的map计算
 */

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
public class HdfsStreamAccess {
    private FileSystem fs = null;
    private Configuration conf = null;

    @Before
    public void init() throws Exception{


        //可以直接传入 uri和用户身份
        fs = FileSystem.get(new URI("hdfs://mini1:9000"),new Configuration(),"hadoop");
    }


    /**
     * 通过流的方式上传文件到hdfs
     *
     */
    @Test
    public void testUpload() throws Exception {
         FSDataOutputStream os = fs.create(new Path("/test"), true);
         FileInputStream is = new FileInputStream("c:/test.txt");
         IOUtils.copy(is,os);
         if(os!=null){
             os.close();
         }
         if(is!=null){
             is.close();
         }
    }


    /**
     * 通过流的方式获取hdfs上数据
     *
     */
    @Test
    public void testDownLoad() throws Exception {
        FSDataInputStream is = fs.open(new Path("/test.txt")); fs.open(new Path("/test.txt"));
        FileOutputStream os =new FileOutputStream("d:/");
        IOUtils.copy(is,os);
        if(os!=null){
            os.close();
        }
        if(is!=null){
            is.close();
        }

    }


    @Test
    public void testRandomAccess() throws Exception{
        FSDataInputStream is = fs.open(new Path("/test.txt"));
        //偏置12个字节输入
        is.seek(12);
        FileOutputStream os = new FileOutputStream("d:test.txt");
        IOUtils.copy(is,os);
        if(os!=null){
            os.close();
        }
        if(is!=null){
            is.close();
        }
    }



    /**
     * 显示hdfs上文件的内容
     *
     */
    @Test
    public void testCat() throws IllegalArgumentException, IOException{

        FSDataInputStream in = fs.open(new Path("/angelababy.love"));

        IOUtils.copy(in, System.out);
    }

}
