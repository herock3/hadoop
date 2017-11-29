package cn.zyx.hadoop.rpc.client;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import cn.zyx.hadoop.rpc.protocol.ClientNameNodeProtocol;


public class MyHdfsClient {

	public static void main(String[] args) throws Exception {
		ClientNameNodeProtocol namenode =(ClientNameNodeProtocol) RPC.getProxy(ClientNameNodeProtocol.class, 1L,
				new InetSocketAddress("localhost", 8888), new Configuration());
		String metaData = namenode.getMetaData("/angela.mygirl");
		System.out.println(metaData);
	}

}
