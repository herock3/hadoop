package cn.zyx.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 相当于一个yarn客户端
 * 需要再次封装我们的mr程序的的相关运行参数，指定jar包，最后提交给yarn
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        //指定本程序的Jar包坐在的本地路径
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定最终输出的数据的KV类型，即是reduce的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定job的输入原始文件坐在的目录
        FileInputFormat.setInputPaths(job,new Path("/wordcount/input"));
        //指定job的输出结果文件坐在的目录
        FileOutputFormat.setOutputPath(job,new Path("/wordcount/output"));
        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
//        job.submit();
        boolean res = job.waitForCompletion(true);
        System.exit(res?1:0);
    }
}
