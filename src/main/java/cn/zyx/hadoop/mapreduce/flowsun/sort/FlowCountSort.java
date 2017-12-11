package cn.zyx.hadoop.mapreduce.flowsun.sort;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//club.drguo.mapreduce.flowcount.FlowCountSort
public class FlowCountSort {
    public static class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable>{
        private FlowBean bean = new FlowBean();
        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, FlowBean, NullWritable>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] strings = StringUtils.split(line, "\t");

            String phoneNum = strings[0];
            long up_flow  = Long.parseLong(strings[1]);
            long down_flow  = Long.parseLong(strings[2]);

            bean.set(phoneNum, up_flow, down_flow);
            context.write(bean, NullWritable.get());
        }
    }
    public static class FlowCountSortReducer extends Reducer<FlowBean, NullWritable, Text, FlowBean>{
        @Override
        protected void reduce(FlowBean bean, Iterable<NullWritable> values,
                              Reducer<FlowBean, NullWritable, Text, FlowBean>.Context context) throws IOException, InterruptedException {
            context.write(new Text(bean.getPhoneNum()), bean);
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "sortjob");
        job.setJarByClass(FlowCountSort.class);

        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        // 可以不写，默认就是下面的
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //没写死，输命令时自己写
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.out.println(b ? "完成" : "未完成");
    }
}
