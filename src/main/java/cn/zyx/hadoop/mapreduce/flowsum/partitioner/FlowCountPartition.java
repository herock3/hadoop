package cn.zyx.hadoop.mapreduce.flowsum.partitioner;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class FlowCountPartition {
    public static class FlowCountPartitionMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        // 减少内存占用（如果放下面，GC机制会使对象越积越多）
        private FlowBean flowBean = new FlowBean();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
                throws IOException, InterruptedException {
            try {
                // 拿到一行数据
                String line = value.toString();
                // 切分字段
                String[] strings = StringUtils.split(line, "\t");
                // 拿到我们需要的若干个字段
                String phoneNum = strings[1];
                long up_flow = Long.parseLong(strings[strings.length - 3]);
                long down_flow = Long.parseLong(strings[strings.length - 2]);
                // 将数据封装到一个flowbean中
                flowBean.set(phoneNum, up_flow, down_flow);
                // 以手机号为key，将流量数据输出去
                context.write(new Text(phoneNum), flowBean);
            } catch (Exception e) {
                System.out.println("-----------------mapper出现问题");
            }
        }
    }

    // 出去的是手机号和flowbean
    public static class FlowCountPartitionReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        // 减少内存占用（如果放下面，GC机制会使对象越积越多）
        private FlowBean flowBean = new FlowBean();

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values,
                              Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
            long up_flow_sum = 0;
            long down_flow_sum = 0;
            for (FlowBean bean : values) {
                up_flow_sum += bean.getUp_flow();
                down_flow_sum += bean.getDown_flow();
            }
            flowBean.set(key.toString(), up_flow_sum, down_flow_sum);
            context.write(key, flowBean);
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "flowpartjob");
        job.setJarByClass(FlowCountPartition.class);

        job.setMapperClass(FlowCountPartitionMapper.class);
        job.setReducerClass(FlowCountPartitionReducer.class);

        /**
         * 加入自定义分区定义：AreaPartitioner
         */
        job.setPartitionerClass(AreaPartitioner.class);

        /**
         * 设置reduce task的数量，要跟AreaPartitioner返回的partitioner个数匹配
         * 若reduce task多，会产生多余的几个空文件
         * 若reduce task少，就会发生异常，因为有一些key没有对应reduce task接收
         * 但reduce task数量为1时，不会产生异常，因为所有key都会给这一个reduce task
         * reduce task和map task指的是reducer和mapper在集群中运行的实例
         */
        job.setNumReduceTasks(5);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

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