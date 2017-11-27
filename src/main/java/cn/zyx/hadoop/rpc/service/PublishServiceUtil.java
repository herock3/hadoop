package cn.zyx.hadoop.rpc.service;
import cn.zyx.hadoop.rpc.protocol.ClientNameNodeProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import java.io.IOException;

public class PublishServiceUtil {
    public static void main(String[] args) throws IOException {
        Builder builder = new Builder(new Configuration());
        builder.setBindAddress("localhost")
                .setPort(8888)
                .setProtocol(ClientNameNodeProtocol.class)
                .setInstance(new MyNameNode());
        RPC.Server server= builder.build();
        server.start();
    }
}
