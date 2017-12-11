package cn.zyx.hadoop.mapreduce.WordCountDemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Text key, Iterable<IntWritable> values 对应Map的输出
 * Text,IntWritable 对应最后reduce输出的结果
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    /**
     * 输入参数，是一组相同单词的kv的key
     * 每调用一次reduce就处理一组相同的单词，如统计一次hello的次数就要调用一次reduce
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;
        for(IntWritable value:values){
            count += value.get();
        }
        context.write(key,new IntWritable(count));
    }
}