package dc;

import java.io.Serializable;

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
  
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append("\nStateType: " + type);
    result.append("\nUpset: ");
    for (int i = 0; i < upset.length; i++) {
      result.append("\t" + i + ":" + upset[i]);
    }
    return result.toString();
  }
  
  private StateType type;
  private boolean[] upset;
  
  public static boolean isTerminalStateType(StateType st) {
    return (st == StateType.COMMITED || st == StateType.ABORTED);
  }
}
