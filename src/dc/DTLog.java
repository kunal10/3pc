package dc;

/**
 * Format of DT Log :
 * Start Transaction
 * Up Set: 1,2,3
 * Decision: ABORT/COMMIT
 * End Transaction
 * @author av28895
 *
 */
public class DTLog {

  public DTLog(int processId, int fileName) {
    super();
    this.processId = processId;
    this.fileName = fileName;
  }
  public int getProcessId() {
    return processId;
  }
  public void setProcessId(int processId) {
    this.processId = processId;
  }
  public int getFileName() {
    return fileName;
  }
  public void setFileName(int fileName) {
    this.fileName = fileName;
  }
  
  public State retrieveProcessState(){
    return null;
    
  }
  /**
   * The process ID which uses this DT log.
   */
  private int processId;
  /**
   * File name of the DT log.
   */
  private int fileName;
}
