package ac.cn.saya.etl2;

/**
 * @Title: LogBean
 * @ProjectName haddop1
 * @Description: TODO
 * @Author Saya
 * @Date: 2018/8/20 22:43
 * @Description:
 */

public class LogBean {

    private String remoteAddr;// 记录客户端的 ip 地址
    private String remoteUser;// 记录客户端用户名称,忽略属性"-"
    private String timeLocal;// 记录访问时间与时区
    private String request;// 记录请求的 url 与 http 协议
    private String status;// 记录请求状态；成功是 200
    private String bodyBytesSent;// 记录发送给客户端文件主体内容大小
    private String httpReferer;// 用来记录从那个页面链接访问过来的
    private String httpUserAgent;// 记录客户浏览器的相关信息
    private boolean valid = true;// 判断数据是否合法

    public LogBean() {
    }

    public String getRemoteAddr() {
        return remoteAddr;
    }

    public void setRemoteAddr(String remoteAddr) {
        this.remoteAddr = remoteAddr;
    }

    public String getRemoteUser() {
        return remoteUser;
    }

    public void setRemoteUser(String remoteUser) {
        this.remoteUser = remoteUser;
    }

    public String getTimeLocal() {
        return timeLocal;
    }

    public void setTimeLocal(String timeLocal) {
        this.timeLocal = timeLocal;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBodyBytesSent() {
        return bodyBytesSent;
    }

    public void setBodyBytesSent(String bodyBytesSent) {
        this.bodyBytesSent = bodyBytesSent;
    }

    public String getHttpReferer() {
        return httpReferer;
    }

    public void setHttpReferer(String httpReferer) {
        this.httpReferer = httpReferer;
    }

    public String getHttpUserAgent() {
        return httpUserAgent;
    }

    public void setHttpUserAgent(String httpUserAgent) {
        this.httpUserAgent = httpUserAgent;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.valid);
        sb.append("\001").append(this.remoteAddr);
        sb.append("\001").append(this.remoteUser);
        sb.append("\001").append(this.timeLocal);
        sb.append("\001").append(this.request);
        sb.append("\001").append(this.status);
        sb.append("\001").append(this.bodyBytesSent);
        sb.append("\001").append(this.httpReferer);
        sb.append("\001").append(this.httpUserAgent);
        return sb.toString();
    }
}
