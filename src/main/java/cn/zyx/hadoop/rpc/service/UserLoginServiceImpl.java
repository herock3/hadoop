package cn.zyx.hadoop.rpc.service;

import cn.zyx.hadoop.rpc.protocol.IuserLoginService;

public class UserLoginServiceImpl implements IuserLoginService {

    public String login(String name, String passwd) {
        return name + "logged in successfully...";
    }
}
