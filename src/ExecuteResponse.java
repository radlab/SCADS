package deploylib;

public class ExecuteResponse {
    private int exitStatus;
    private String stdout;
    private String stderr;
    
    public ExecuteResponse(int exitStatus, String stdout, String stderr) {
        this.exitStatus = exitStatus;
        this.stdout = stdout;
        this.stderr = stderr;
    }
    
    public int getExitStatus() {
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