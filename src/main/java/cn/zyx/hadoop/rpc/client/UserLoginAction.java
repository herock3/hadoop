package cn.zyx.hadoop.rpc.client;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import cn.zyx.hadoop.rpc.protocol.IuserLoginService;


public class UserLoginAction {
	public static void main(String[] args) throws Exception {
		IuserLoginService userLoginService = RPC.getProxy(IuserLoginService.class, 100L, new InetSocketAddress("localhost", 9999), new Configuration());
		String login = userLoginService.login("angelababy", "1314520");
		System.out.println(login);
		
	}
}
