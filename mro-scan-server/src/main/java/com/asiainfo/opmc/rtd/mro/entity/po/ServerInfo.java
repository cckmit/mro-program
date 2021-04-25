package com.asiainfo.opmc.rtd.mro.entity.po;

/**
 * @program: cmzj-data-mro-parent
 * @description: 服务器信息
 * @author: sd
 * @create: 2021-03-11 15:37
 **/
public class ServerInfo {
    /**
     * TS服务器IP
     */
    String hostName;
    /**
     * 端口
     */
    Integer port;
    /**
     * 登录用户用户名
     */
    String userName;
    /**
     * 登录用户密码
     */
    String password;

    /**
     * 采集文件路径
     */
    String basePath;


    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getBasePath() {
        return basePath;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    public ServerInfo(String hostName, Integer port, String userName, String password) {
        this.hostName = hostName;
        this.port = port;
        this.userName = userName;
        this.password = password;
    }

    public ServerInfo(String hostName, Integer port, String userName, String password, String basePath) {
        this.hostName = hostName;
        this.port = port;
        this.userName = userName;
        this.password = password;
        this.basePath = basePath;
    }

    @Override
    public String toString() {
        return "ServerInfo{" +
                "hostName='" + hostName + '\'' +
                ", port=" + port +
                ", userName='" + userName + '\'' +
                ", password='" + password + '\'' +
                ", basePath='" + basePath + '\'' +
                '}';
    }
}
