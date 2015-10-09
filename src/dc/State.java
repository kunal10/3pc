package dc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class State implements Serializable{
  private static final long serialVersionUID = 1L;

  /**
   * @param type
   * @param upset
   */
  public State(StateType type, boolean[] upset) {
    super();
    this.type = type;
    this.upset = new boolean[upset.length];
    System.arraycopy(upset, 0, this.upset, 0, upset.length);
  }
  public State(State other) {
    this(other.getType(), other.getUpset());
  }
  
  public enum StateType { UNCERTAIN, COMMITABLE, COMMITED, ABORTED }; 
  public boolean isTerminalState() {
    return isTerminalStateType(type);
  } 
  public void setType(StateType st) {
    type = st;
  }
  public StateType getType() {
    return type;
  }
  public boolean[] getUpset() {
    return upset;
  }
  public void removeProcessFromUpset(int i){
    upset[i] = false;
  }
  @Override
  public String toString() {
    String stateType = type.toString()+"#";
    for(int i = 0; i < upset.length; i++){
      stateType += (upset[i]) ? 1 : 0;
    }
    return stateType;
  }
  
  public static State parseState(String s) throws Exception{
    String[] split = s.split("#");
    State parsedState = null;
    if(split.length != 2){
      throw new Exception("Error in parsing state : "+s);
    }
    else{
      StateType stateType = StateType.valueOf(split[0].trim());
      ArrayList<Boolean> upset = new ArrayList<>();
      for (int i = 0; i < split[1].length(); i++) {
        boolean c = (split[1].charAt(i) == '1') ; 
        upset.add(c);
      }
      int size = upset.size();
      boolean[] createdUpset = new boolean[size];
      for (int i = 0; i< size; i++) {
        createdUpset[i] = upset.get(i).booleanValue();
      }
      parsedState = new State(stateType, createdUpset);
    }
    return parsedState;
  }
  

/*  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append("\nStateType: " + type);
    result.append("\nUpset: ");
    for (int i = 0; i < upset.length; i++) {
      result.append("\t" + i + ":" + upset[i]);
    }
    return result.toString();
  }
*/  
  
  public static boolean isTerminalStateType(StateType st) {
    return (st == StateType.COMMITED || st == StateType.ABORTED);
  }
  
  private StateType type;
  private boolean[] upset;
  
  public void setUpset(boolean[] upset) {
    this.upset = Arrays.copyOf(upset, upset.length);
  }
  
  public static void main(String[] args){
    boolean[] b = {true, false, false, true};
    State dc = new State(StateType.ABORTED, b);
    System.out.println(dc.toString());
    try {
      System.out.println(State.parseState(dc.toString()).toString());
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
