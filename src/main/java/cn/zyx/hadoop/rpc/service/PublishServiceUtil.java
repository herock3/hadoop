package cn.zyx.hadoop.rpc.service;
import cn.zyx.hadoop.rpc.protocol.ClientNameNodeProtocol;
import cn.zyx.hadoop.rpc.protocol.IuserLoginService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.junit.Before;
import org.junit.Test;
import org.omg.CORBA.INITIALIZE;

import java.io.IOException;

public class PublishServiceUtil {
    private static Builder builder=null;
    private static final String ADDRESS="localhost";

    public static void init(){
      builder = new Builder(new Configuration());
      builder.setBindAddress(ADDRESS);
    }


    public static void nameNodeProtocol() throws IOException {
        builder.setPort(8888)
                .setProtocol(ClientNameNodeProtocol.class)
                .setInstance(new MyNameNode());
        RPC.Server server = builder.build();
        server.start();
    }


    public static void userLoginService() throws IOException {
        builder.setPort(9999)
                .setProtocol(IuserLoginService.class)
                .setInstance(new UserLoginServiceImpl());
        RPC.Server server = builder.build();
        server.start();
    }

    public static void main(String[] args) throws IOException {
        init();
        nameNodeProtocol();
        userLoginService();
    }

}
