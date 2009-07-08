package deploylib;

public class ExecuteResponse {
    private Integer exitStatus;
    private String stdout;
    private String stderr;
    
    public ExecuteResponse(Integer exitStatus, String stdout, String stderr) {
        this.exitStatus = exitStatus;
        this.stdout = stdout;
        this.stderr = stderr;
    }
    
    public Integer getExitStatus() {
        return exitStatus;
    }
    
    public String getStdout() {
        return stdout;
    }
    
    public String getStderr() {
        return stderr;
    }
    
    public String toString() {
        return ("Exit status: " + exitStatus + "\nstdout: " + stdout +
                "\nstderr: " + stderr);
    }
}