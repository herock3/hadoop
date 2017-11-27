package cn.zyx.hadoop.rpc.service;

import cn.zyx.hadoop.rpc.protocol.ClientNameNodeProtocol;

public class MyNameNode implements ClientNameNodeProtocol {
    public String getMetaData(String path) {
        return path+": 3 - {BLK_1,BLK_2} ....";
    }
}
