package cn.zyx.hadoop.rpc.protocol;

public interface IuserLoginService {
    public static final long versionID=100L;
    public String login(String name,String passwd);
}
