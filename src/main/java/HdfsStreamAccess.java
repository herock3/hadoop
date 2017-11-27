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



    }


    /**
     * 通过流的方式获取hdfs上数据
     *
     */
    @Test
    public void testDownLoad() throws Exception {


    }


    @Test
    public void testRandomAccess() throws Exception{




    }



    /**
     * 显示hdfs上文件的内容
     *
     */
    @Test
    public void testCat() throws IllegalArgumentException, IOException{



    }

}
