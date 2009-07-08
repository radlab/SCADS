package deploylib;


public class ServiceStatus {
    private String status;
    private String id;
    private int pid;
    private int uptime;
    
    public ServiceStatus(String status, String id, int pid, int uptime) {
        this.status = status;
        this.id     = id;
        this.pid    = pid;
        this.uptime = uptime;
    }
    
    public String getStatus() {
        return status;
    }
    
    public String getId() {
        return id;
    }
    
    public int getPid() {
        return pid;
    }
    
    public int getUptime() {
        return uptime;
    }
    
    public String toString() {
        return ("Status: " + status + "\nID: " + id + "\nPID: "+ pid +
                "\nUptime: " + uptime);
    }
}