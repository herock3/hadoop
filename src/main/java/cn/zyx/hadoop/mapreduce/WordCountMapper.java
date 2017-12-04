package cn.zyx.hadoop.mapreduce;
/***
 * KEYIN:默认情况下，是mr框架所读到的一行文本的偏移量：Long，但在hadoopz中有自己更精简的序列化接口，所以不用Long,而用LongWritable
 * VALUEIN:默认情况下，是mr框架所读到的一行文本的内容：String    Text
 * KEYOUT：是用户自定义的逻辑处理完成之后输出数据的key，此处是单词，String   Text
 * VALUEOUT:是用户自定义逻辑处理完成之后输出的数据中的value,此处是单词的次数，Integer   IntegerWritable
 *
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    /**
     * map阶段的有任务逻辑就写在自定义的map()方法中
     * maptask会对每一行输入数据调用一次我们自定义的map()方法
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        //将单词输入<单词，1>,以单词作为key，将1作为value
        for(String word:words){
            context.write(new Text(word),new IntWritable(1));
        }

    }
}
