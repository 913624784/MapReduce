package Work1;

public class KPIBean {
    //判断该记录格式是否正确
    private Boolean is_validate = true;
    //需要解析的属性为8个
    private String ip;//用户ip,0
    private String user;//客户端用户名,1
    private String time;//请求时间,3
    private String method;//请求方法,5
    private String page;// 请求页面,6
    private String http;// http协议信息,7
    private String status;//返回的状态码,8
    private String sent_bytes;//发送的页面字节数,9
    private String httpfrom;//从什么页面跳转进来,10

    public static KPIBean parse(String line) throws Exception {
        String str[] = line.split(" ");
        KPIBean kpi = new KPIBean();
        kpi.setIp(str[0]);
        kpi.setUser(str[1]);
        kpi.setTime(str[3].substring(1));
        kpi.setMethod(str[5].substring(1));
        kpi.setPage(str[6]);
        kpi.setHttp(str[7].substring(0, str[7].length() - 1));
        kpi.setStatus(str[8]);
        kpi.setSent_bytes(str[9]);
        kpi.setHttpfrom(str[10]);
        if (!kpi.getStatus().equals("") && Integer.parseInt(kpi.getStatus()) > 400) {
            kpi.setIs_validate(false);
            throw new Exception("数据有错误！");
        } else {
            kpi.setIs_validate(true);
        }
        return kpi;
    }

    @Override
    public String toString() {
        return "KPI{" +
                "is_validate=" + is_validate +
                ", ip='" + ip + '\'' +
                ", user='" + user + '\'' +
                ", time='" + time + '\'' +
                ", method='" + method + '\'' +
                ", page='" + page + '\'' +
                ", http='" + http + '\'' +
                ", status='" + status + '\'' +
                ", sent_bytes='" + sent_bytes + '\'' +
                ", httpfrom='" + httpfrom + '\'' +
                '}';
    }

    public Boolean getIs_validate() {
        return is_validate;
    }

    public void setIs_validate(Boolean is_validate) {
        this.is_validate = is_validate;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public String getHttp() {
        return http;
    }

    public void setHttp(String http) {
        this.http = http;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getSent_bytes() {
        return sent_bytes;
    }

    public void setSent_bytes(String sent_bytes) {
        this.sent_bytes = sent_bytes;
    }

    public String getHttpfrom() {
        return httpfrom;
    }

    public void setHttpfrom(String httpfrom) {
        this.httpfrom = httpfrom;
    }


}
