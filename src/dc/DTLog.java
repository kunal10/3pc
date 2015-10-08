package dc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.logging.Level;

import dc.State.StateType;
import ut.distcomp.framework.Config;

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

  public DTLog(Config config) {
    super();
    this.config = config;
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
    try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(config.DTFilename, true)))) {
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
    try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(config.DTFilename, true)))) {
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
    try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(config.DTFilename, true)))) {
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
    try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(config.DTFilename, true)))) {
      // TODO: Check if there is a decision already for that transaction
      out.println("State :"+s.toString());
    }catch (IOException e) {
        // Handle
    }
  }
  
  /**
   * Write the playlist into the DT log.
   * @param pl
   */
  public void writePlaylist(Playlist pl){
    try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(config.DTFilename, true)))) {
      // TODO: Check if there is a decision already for that transaction
      out.println("Playlist :"+pl.toString());
    }catch (IOException e) {
        // Handle
    }
  }
  
  public RecoveredState parseDTLog() throws FileNotFoundException, IOException {
    RecoveredState rs = new RecoveredState();
    try(BufferedReader br = new BufferedReader(new FileReader(config.DTFilename))) {
      String line = br.readLine();
      while(line != null){
        if(line.startsWith("Start")){
          rs.state.setUpset(new boolean[config.numProcesses - 1]);
          rs.state.setType(StateType.UNCERTAIN);
          rs.decision = "";
          config.logger.info("Start DT : "+ rs.toString());
        }
        else if(line.startsWith("End")){
          config.logger.info("End DT : "+ rs.toString());
        }
        else if(line.startsWith("Decision")){
          rs.decision = line.split(":")[1]; 
        }
        else if(line.startsWith("State")){
          try {
            rs.state = State.parseState(line.split(":")[1]);
          } catch (Exception e) {
            config.logger.info("Couldn't parse state");
          }
        }
        else if(line.startsWith("Playlist")){
          
        }
        else{
          config.logger.log(Level.SEVERE, "Couldn't parse "+ line + " in DT Log");
        }
          
        line = br.readLine();
      }
      
    }
    return null;
    
  }
  
  
  class RecoveredState{
    public State state;
    public Playlist playlist;
    public String decision;
    public RecoveredState() {
      state = new State(StateType.UNCERTAIN, new boolean[config.numProcesses - 1]);
      playlist = new Playlist();
      decision = "";
    }
    @Override
    public String toString() {
      // TODO Auto-generated method stub
      return "DT Log State : "+state.toString()+"..."+playlist.toString()+"..."+decision;
    }
  }
  
  /*
   * Config for the process owning this DT log.
   */
  private Config config;
}
