package cn.zyx.hadoop.mapreduce.flowsun.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//实现序列化接口
public class FlowBean implements WritableComparable<FlowBean>{
    //手机号
    private String phoneNum;
    //上传流量
    private long up_flow;
    //下载流量
    private long down_flow;
    //总流量
    private long sum_flow;

    public void set(String phoneNum, long up_flow, long down_flow){
        this.phoneNum = phoneNum;
        this.up_flow = up_flow;
        this.down_flow = down_flow;
        this.sum_flow = up_flow + down_flow;
    }
    /**
     * 序列化，将数据字段以字节流写出去
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phoneNum);
        out.writeLong(up_flow);
        out.writeLong(down_flow);
        out.writeLong(sum_flow);
    }
    /**
     * 反序列化，从字节流中读出各个数据字段
     * 读写顺序，数据类型应一致
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        phoneNum = in.readUTF();
        up_flow = in.readLong();
        down_flow = in.readLong();
        sum_flow = in.readLong();
    }

    public String getPhoneNum() {
        return phoneNum;
    }
    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }
    public long getUp_flow() {
        return up_flow;
    }
    public void setUp_flow(long up_flow) {
        this.up_flow = up_flow;
    }
    public long getDown_flow() {
        return down_flow;
    }
    public void setDown_flow(long down_flow) {
        this.down_flow = down_flow;
    }
    public long getSum_flow() {
        return sum_flow;
    }
    public void setSum_flow(long sum_flow) {
        this.sum_flow = sum_flow;
    }
    //不写结果会出问题
    @Override
    public String toString() {
        return up_flow + "\t" + down_flow + "\t" + sum_flow;
    }
    //比较排序（总流量）
    @Override
    public int compareTo(FlowBean o) {
        return this.sum_flow > o.getSum_flow()?-1:1;
    }

}