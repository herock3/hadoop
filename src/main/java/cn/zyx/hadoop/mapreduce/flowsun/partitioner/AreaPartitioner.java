package cn.zyx.hadoop.mapreduce.flowsun.partitioner;

import java.util.HashMap;

import org.apache.hadoop.mapreduce.Partitioner;

public class AreaPartitioner<KEY, VALUE> extends Partitioner<KEY, VALUE> {

    //手机号，地区代码
    private static HashMap<String, Integer> areaMap = new HashMap<String, Integer>();
    //静态代码块，将数据先加载到内存中
    static{
        areaMap.put("134", 0);
        areaMap.put("135", 1);
        areaMap.put("137", 2);
        areaMap.put("138", 3);
    }
    @Override
    public int getPartition(KEY key, VALUE value, int numPartitions) {
        Integer provinceCode = areaMap.get(key.toString().substring(0,3));

        return provinceCode==null?4:provinceCode;
    }

}
