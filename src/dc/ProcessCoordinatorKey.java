package dc;

public class ProcessCoordinatorKey {
    public ProcessCoordinatorKey(Integer pid, Integer cid) {
		super();
		this.pid = pid;
		this.cid = cid;
	}

	private Integer pid;
    private Integer cid;

    @Override
    public boolean equals(Object obj) {
        if(obj != null && obj instanceof ProcessCoordinatorKey) {
            ProcessCoordinatorKey s = (ProcessCoordinatorKey)obj;
            return pid.equals(s.pid) && cid.equals(s.cid);
        }
        return false;
    }

    @Override
    public int hashCode() {
    	int result = pid;
        result = 31 * result + cid;
        return result;
    }

	public Integer getPid() {
		return pid;
	}

	public void setPid(Integer pid) {
		this.pid = pid;
	}

	public Integer getCid() {
		return cid;
	}

	public void setCid(Integer cid) {
		this.cid = cid;
	}
}
