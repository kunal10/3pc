package dc;

public class State {
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
    return (type == StateType.COMMITED || type == StateType.ABORTED);
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
  
  private StateType type;
  private boolean[] upset;
}
