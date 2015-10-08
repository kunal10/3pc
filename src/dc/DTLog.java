package dc;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

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

  public DTLog(int processId, String fileName) {
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
  public String getFileName() {
    return fileName;
  }
  public void setFileName(String fileName) {
    this.fileName = fileName;
  }
  
  /**
   * Parse the DT Log and return the state of the process.
   * @return
   */
  public State retrieveProcessState(){
    return null;
  }
  
  /**
   * Write a start transaction line.
   */
  public void writeStartTransaction(){
    try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(fileName, true)))) {
      out.println("Start Transaction");
      // TODO: Write upset.
    }catch (IOException e) {
        // Handle
    }
  }
  
  /**
   * Write an end transaction line.
   */
  public void writeEndTransaction(){
    try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(fileName, true)))) {
      out.println("End Transaction");
    }catch (IOException e) {
        // Handle
    }
  }
  
  /**
   * Write the decision taken by a process for a transaction.
   * @param decision
   */
  public void writeDecision(String decision){
    try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(fileName, true)))) {
      // TODO: Check if there is a decision already for that transaction
      out.println("Decision :"+decision);
    }catch (IOException e) {
        // Handle
    }
  }
  
  /**
   * Write the state of the process. Should be written everytime the state changes.
   * @param s
   */
  public void writeState(State s){
    
  }
  /**
   * The process ID which uses this DT log.
   */
  private int processId;
  /**
   * File name of the DT log.
   */
  private String fileName;
}
